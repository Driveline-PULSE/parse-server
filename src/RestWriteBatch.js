// A RestWriteBatch encapsulates everything we need to run a batch
// create operation that writes multiple objects to the database.
// This is optimized for batch inserts and is NOT used for updates.

var SchemaController = require('./Controllers/SchemaController');
var deepcopy = require('deepcopy');

var cryptoUtils = require('./cryptoUtils');
var Parse = require('parse/node');
var triggers = require('./triggers');
var ClientSDK = require('./ClientSDK');
import _ from 'lodash';
import { createSanitizedError } from './Error';

// dataArray is an array of REST API format objects to create.
// RestWriteBatch will handle objectId, createdAt, and updatedAt for
// each object. It also knows to use triggers for the class.
function RestWriteBatch(config, auth, className, dataArray, clientSDK, context) {
  if (auth.isReadOnly) {
    throw createSanitizedError(
      Parse.Error.OPERATION_FORBIDDEN,
      'Cannot perform a write operation when using readOnlyMasterKey',
      config
    );
  }
  this.config = config;
  this.auth = auth;
  this.className = className;
  this.clientSDK = clientSDK;
  this.storage = {};
  this.runOptions = {};
  this.context = context || {};

  const objectIdFound = dataArray.find(data => data.objectId) !== undefined;

  if (objectIdFound && !this.config.allowCustomObjectId) {
    throw new Parse.Error(Parse.Error.INVALID_KEY_NAME, 'objectId is an invalid field name.');
  }

  // When the operation is complete, this.response may have several fields.
  // response: the actual data to be returned
  // status: the http status code. if not present, treated like a 200
  // location: the location header. if not present, no location header
  this.response = null;

  // Processing this operation may mutate our data, so we operate on a copy
  this.dataArray = deepcopy(dataArray);

  // The timestamp we'll use for this whole operation
  this.updatedAt = Parse._encode(new Date()).iso;

  // Shared SchemaController to be reused
  this.validSchemaController = null;
}

// A convenient method to perform all the steps of processing the
// write, in order.
// Returns a promise for a {response, status, location} object.
RestWriteBatch.prototype.execute = async function () {
  await this.getUserAndRoleACL();
  await this.validateClientClassCreation();
  await this.runBeforeTrigger();
  await this.validateSchema();
  await this.setRequiredFieldsIfNeeded();
  await this.expandFilesForExistingObjects();
  await this.runDatabaseOperation();
  await this.runAfterTrigger();
  return this.response;
};

// Uses the Auth object to get the list of roles, adds the user id
RestWriteBatch.prototype.getUserAndRoleACL = async function () {
  if (this.auth.isMaster || this.auth.isMaintenance) {
    return;
  }

  this.runOptions.acl = ['*'];

  if (this.auth.user) {
    const roles = await this.auth.getUserRoles();
    this.runOptions.acl = this.runOptions.acl.concat(roles, [this.auth.user.id]);
  }
};

// Validates this operation against the allowClientClassCreation config.
RestWriteBatch.prototype.validateClientClassCreation = async function () {
  if (
    this.config.allowClientClassCreation === false &&
    !this.auth.isMaster &&
    !this.auth.isMaintenance &&
    SchemaController.systemClasses.indexOf(this.className) === -1
  ) {
    const schemaController = await this.config.database.loadSchema();
    const hasClass = await schemaController.hasClass(this.className);
    if (hasClass !== true) {
      throw createSanitizedError(
        Parse.Error.OPERATION_FORBIDDEN,
        'This user is not allowed to access non-existent class: ' + this.className,
        this.config
      );
    }
  }
};

// Validates this operation against the schema.
RestWriteBatch.prototype.validateSchema = async function () {
  // Validate each object in the batch
  for (const data of this.dataArray) {
    await this.config.database.validateObject(
      this.className,
      data,
      null,
      this.runOptions,
      this.auth.isMaintenance
    );
  }
};

// Runs any beforeSave triggers against this operation.
// Any change leads to our data being mutated.
RestWriteBatch.prototype.runBeforeTrigger = async function () {
  if (this.response) {
    return;
  }

  // Avoid doing any setup for triggers if there is no 'beforeSave' trigger for this class.
  if (
    !triggers.triggerExists(this.className, triggers.Types.beforeSave, this.config.applicationId)
  ) {
    return;
  }

  const promises = this.dataArray.map(async (data, index) => {
    // Cloud code gets a bit of extra data for its objects
    const extraData = { className: this.className };

    const updatedObject = triggers.inflate(extraData, null);
    updatedObject.set(this.sanitizedData(data));

    const response = await triggers.maybeRunTrigger(
      triggers.Types.beforeSave,
      this.auth,
      updatedObject,
      null,
      this.config,
      this.context
    );

    if (response && response.object) {
      this.storage.fieldsChangedByTrigger = _.reduce(
        response.object,
        (result, value, key) => {
          if (!_.isEqual(this.dataArray[index][key], value)) {
            result.push(key);
          }
          return result;
        },
        []
      );
      this.dataArray[index] = response.object;
    }
  });

  await Promise.all(promises);
};

RestWriteBatch.prototype.setRequiredFieldsIfNeeded = async function () {
  if (!this.dataArray) return;

  // Add default fields
  this.dataArray.forEach(data => {
    data.updatedAt = this.updatedAt;
    data.createdAt = this.updatedAt;

    // Only assign new objectId if we are creating new object
    if (!data.objectId) {
      data.objectId = cryptoUtils.newObjectId(this.config.objectIdSize);
    }
  });
};

// If we short-circuited the object response - then we need to make sure we expand all the files,
// since this might not have a query, meaning it won't return the full result back.
RestWriteBatch.prototype.expandFilesForExistingObjects = async function () {
  // Check whether we have a short-circuited response - only then run expansion.
  if (this.response && this.response.response) {
    await this.config.filesController.expandFilesInObject(this.config, this.response.response);
  }
};

RestWriteBatch.prototype.runDatabaseOperation = async function () {
  if (this.response) {
    return;
  }

  // Run a batch create
  try {
    const responses = await this.config.database.createMultiple(
      this.className,
      this.dataArray,
      this.runOptions,
      false,
      this.validSchemaController
    );

    responses.forEach((response, index) => {
      response.objectId = this.dataArray[index].objectId;
      response.createdAt = this.dataArray[index].createdAt;
      this._updateResponseWithData(response, this.dataArray[index]);
    });

    this.response = {
      status: 201,
      response: responses,
      location: this.location(),
    };
  } catch (error) {
    if (error.code !== Parse.Error.DUPLICATE_VALUE) {
      throw error;
    }
    // If duplicate value error, still throw to inform the caller
    throw error;
  }
};

// Returns nothing - doesn't wait for the trigger.
RestWriteBatch.prototype.runAfterTrigger = async function () {
  if (!this.response || !this.response.response) {
    return;
  }

  // Avoid doing any setup for triggers if there is no 'afterSave' trigger for this class.
  if (!triggers.triggerExists(this.className, triggers.Types.afterSave, this.config.applicationId)) {
    return;
  }

  const promises = this.dataArray.map(async (data, index) => {
    // Cloud code gets a bit of extra data for its objects
    const extraData = { className: this.className };

    const updatedObject = triggers.inflate(extraData, null);
    updatedObject.set(this.sanitizedData(data));
    updatedObject._handleSaveResponse(this.response.response[index], this.response.status || 200);

    await triggers.maybeRunTrigger(
      triggers.Types.afterSave,
      this.auth,
      updatedObject,
      null,
      this.config,
      this.context
    );
  });

  await Promise.all(promises);
};

// A helper to figure out what location this operation happens at.
RestWriteBatch.prototype.location = function () {
  const middle = '/classes/' + this.className + '/' + 'batch';
  return this.config.mount + middle;
};

// Returns a copy of the data and delete bad keys (_auth_data, _hashed_password...)
RestWriteBatch.prototype.sanitizedData = function (data) {
  const sanitized = Object.keys(data).reduce((result, key) => {
    // Regexp comes from Parse.Object.prototype.validate
    if (/^[A-Za-z][0-9A-Za-z_]*$/.test(key)) {
      result[key] = data[key];
    }
    return result;
  }, {});
  return Parse._decode(undefined, sanitized);
};

RestWriteBatch.prototype._updateResponseWithData = function (response, data) {
  if (_.isEmpty(this.storage.fieldsChangedByTrigger)) {
    return response;
  }
  const clientSupportsDelete = ClientSDK.supportsForwardDelete(this.clientSDK);
  this.storage.fieldsChangedByTrigger.forEach(fieldName => {
    const dataValue = data[fieldName];
    const responseValue = response[fieldName];

    response[fieldName] = responseValue || dataValue;

    // Strips operations from responses
    if (response[fieldName] && response[fieldName].__op) {
      delete response[fieldName];
      if (clientSupportsDelete && dataValue.__op == 'Delete') {
        response[fieldName] = dataValue;
      }
    }
  });
  return response;
};

module.exports = RestWriteBatch;
export default RestWriteBatch;
