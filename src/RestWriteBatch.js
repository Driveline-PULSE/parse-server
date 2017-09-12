'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _RestQuery = require('./RestQuery');

var _RestQuery2 = _interopRequireDefault(_RestQuery);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// A RestWrite encapsulates everything we need to run an operation
// that writes to the database.
// This could be either a "create" or an "update".

var SchemaController = require('./Controllers/SchemaController');
var deepcopy = require('deepcopy');

var Auth = require('./Auth');
var cryptoUtils = require('./cryptoUtils');
var passwordCrypto = require('./password');
var Parse = require('parse/node');
var triggers = require('./triggers');
var ClientSDK = require('./ClientSDK');


// query and data are both provided in REST API format. So data
// types are encoded by plain old objects.
// If query is null, this is a "create" and the data in data should be
// created.
// Otherwise this is an "update" - the object matching the query
// should get updated with data.
// RestWrite will handle objectId, createdAt, and updatedAt for
// everything. It also knows to use triggers and special modifications
// for the _User class.
function RestWriteBatch(config, auth, className, queryArray, dataArray, originalDataArray, clientSDK) {
  this.config = config;
  this.auth = auth;
  this.className = className;
  this.clientSDK = clientSDK;
  this.storage = {};
  this.runOptions = {};

  var objectIdFound = dataArray.find((data) => {
    return data.objectId;
  }) !== undefined;

  if (!queryArray && objectIdFound) {
    throw new Parse.Error(Parse.Error.INVALID_KEY_NAME, 'objectId is an invalid field name.');
  }

  // When the operation is complete, this.response may have several
  // fields.
  // response: the actual data to be returned
  // status: the http status code. if not present, treated like a 200
  // location: the location header. if not present, no location header
  this.response = null;

  // Processing this operation may mutate our data, so we operate on a
  // copy
  this.queryArray = deepcopy(queryArray);
  this.dataArray = deepcopy(dataArray);
  // We never change originalData, so we do not need a deep copy
  this.originalDataArray = originalDataArray;

  // The timestamp we'll use for this whole operation
  this.updatedAt = Parse._encode(new Date()).iso;
}

// A convenient method to perform all the steps of processing the
// write, in order.
// Returns a promise for a {response, status, location} object.
// status and location are optional.
RestWriteBatch.prototype.execute = function () {
  var _this = this;

  return Promise.resolve().then(function () {
    return _this.getUserAndRoleACL();
  }).then(function () {
    return _this.validateClientClassCreation();
  }).then(function () {
    return _this.runBeforeTrigger();
  }).then(function () {
    return _this.validateSchema();
  }).then(function () {
    return _this.setRequiredFieldsIfNeeded();
  }).then(function () {
    return _this.expandFilesForExistingObjects();
  }).then(function () {
    return _this.runDatabaseOperation();
  }).then(function () {
    return _this.runAfterTrigger();
  }).then(function () {
    return _this.response;
  });
};

// Uses the Auth object to get the list of roles, adds the user id
RestWriteBatch.prototype.getUserAndRoleACL = function () {
  var _this2 = this;

  if (this.auth.isMaster) {
    return Promise.resolve();
  }

  this.runOptions.acl = ['*'];

  if (this.auth.user) {
    return this.auth.getUserRoles().then(function (roles) {
      roles.push(_this2.auth.user.id);
      _this2.runOptions.acl = _this2.runOptions.acl.concat(roles);
      return;
    });
  } else {
    return Promise.resolve();
  }
};

// Validates this operation against the allowClientClassCreation config.
RestWriteBatch.prototype.validateClientClassCreation = function () {
  var _this3 = this;

  if (this.config.allowClientClassCreation === false && !this.auth.isMaster && SchemaController.systemClasses.indexOf(this.className) === -1) {
    return this.config.database.loadSchema().then(function (schemaController) {
      return schemaController.hasClass(_this3.className);
    }).then(function (hasClass) {
      if (hasClass !== true) {
        throw new Parse.Error(Parse.Error.OPERATION_FORBIDDEN, 'This user is not allowed to access ' + 'non-existent class: ' + _this3.className);
      }
    });
  } else {
    return Promise.resolve();
  }
};

// Validates this operation against the schema.
RestWriteBatch.prototype.validateSchema = function () {
  this.dataArray.reduce((data, result) => {
    return result && this.config.database.validateObject(this.className, this.data, this.query, this.runOptions);
  }, true)
};

// Runs any beforeSave triggers against this operation.
// Any change leads to our data being mutated.
RestWriteBatch.prototype.runBeforeTrigger = function () {
  var _this4 = this;

  if (this.response) {
    return;
  }

  // Avoid doing any setup for triggers if there is no 'beforeSave' trigger for this class.
  if (!triggers.triggerExists(this.className, triggers.Types.beforeSave, this.config.applicationId)) {
    return Promise.resolve();
  }

  var promises = this.dataArray.map(function(data, index) {
    var query = _this4.queryArray && _this4.queryArray[index] ? _this4.queryArray[index] : null;
    // Cloud code gets a bit of extra data for its objects
    var extraData = { className: _this4.className };
    if (query && query.objectId) {
      extraData.objectId = query.objectId;
    }

    var originalData = _this4.originalData && _this4.originalData[index] ? _this4.originalData[index] : null;

    var originalObject = null;
    var updatedObject = triggers.inflate(extraData, originalData);
    if (query && query.objectId) {
      // This is an update for existing object.
      originalObject = triggers.inflate(extraData, originalData);
    }
    updatedObject.set(_this4.sanitizedData(data));
    return triggers.maybeRunTrigger(triggers.Types.beforeSave, _this4.auth, updatedObject, null, _this4.config).then(function (response) {
      if (response && response.object) {
        _this4.storage.fieldsChangedByTrigger = _lodash2.default.reduce(response.object, function (result, value, key) {
          if (!_lodash2.default.isEqual(_this4.dataArray[index][key], value)) {
            result.push(key);
          }
          return result;
        }, []);
        _this4.dataArray[index] = response.object;
        // We should delete the objectId for an update write
        if (_this4.query && _this4.query.objectId) {
          delete _this4.dataArray[index].objectId;
        }
      }
      return Promise.resolve();
    });
  });
  return Promise.all(promises);
};

RestWriteBatch.prototype.setRequiredFieldsIfNeeded = function () {
  var _this = this;
  if (this.dataArray) {
    // Add default fields
    this.dataArray.forEach(function(data, index) {
      data.updatedAt = _this.updatedAt;
      if (!_this.queryArray) {
        data.createdAt = _this.updatedAt;

        // Only assign new objectId if we are creating new object
        if (!data.objectId) {
          data.objectId = cryptoUtils.newObjectId();
        }
      }
    });
  }
  return Promise.resolve();
};

// If we short-circuted the object response - then we need to make sure we expand all the files,
// since this might not have a query, meaning it won't return the full result back.
// TODO: (nlutsenko) This should die when we move to per-class based controllers on _Session/_User
RestWriteBatch.prototype.expandFilesForExistingObjects = function () {
  // Check whether we have a short-circuited response - only then run expansion.
  if (this.response && this.response.response) {
    this.config.filesController.expandFilesInObject(this.config, this.response.response);
  }
};

RestWriteBatch.prototype.runDatabaseOperation = function () {
  var _this14 = this;

  if (this.response) {
    return;
  }

  if (this.queryArray) {
    // Force the user to not lockout
    // Matched with parse.com

    // Ignore createdAt when update
    delete this.data.createdAt;

    var defer = Promise.resolve();

    return defer.then(function () {
      // Run an update
      return _this14.config.database.update(_this14.className, _this14.query, _this14.data, _this14.runOptions).then(function (response) {
        response.updatedAt = _this14.updatedAt;
        _this14._updateResponseWithData(response, _this14.data);
        _this14.response = { response: response };
      });
    });
  } else {

    // Run a create
    return this.config.database.createMultiple(this.className, this.dataArray, this.runOptions).catch(function (error) {
      if (error.code !== Parse.Error.DUPLICATE_VALUE) {
        throw error;
      }
    }).then(function (responses) {
      responses.forEach((response, index) => {
        response.objectId = _this14.dataArray[index].objectId;
        response.createdAt = _this14.dataArray[index].createdAt;
        _this14._updateResponseWithData(response, _this14.dataArray[index]);
      });
      _this14.response = {
        status: 201,
        response: responses,
        location: _this14.location()
      };
    });
  }
};

// Returns nothing - doesn't wait for the trigger.
RestWriteBatch.prototype.runAfterTrigger = function () {
  if (!this.response || !this.response.response) {
    return;
  }

  // Avoid doing any setup for triggers if there is no 'afterSave' trigger for this class.
  var hasAfterSaveHook = triggers.triggerExists(this.className, triggers.Types.afterSave, this.config.applicationId);
  if (!hasAfterSaveHook) {
    return Promise.resolve();
  }
  var _this = this;
  var promises = this.dataArray.map(function(data, index) {
    var query = _this.queryArray && _this.queryArray[index] ? _this.queryArray[index] : null;
    // Cloud code gets a bit of extra data for its objects
    var extraData = { className: _this.className };
    if (query && query.objectId) {
      extraData.objectId = query.objectId;
    }

    var originalData = _this.originalData && _this.originalData[index] ? _this.originalData[index] : null;

    var originalObject = null;
    var updatedObject = triggers.inflate(extraData, originalData);
    if (query && query.objectId) {
      // This is an update for existing object.
      originalObject = triggers.inflate(extraData, originalData);
    }
    updatedObject.set(_this.sanitizedData(data));
    updatedObject._handleSaveResponse(_this.response.response[index], _this.response.status || 200);

    return triggers.maybeRunTrigger(triggers.Types.afterSave, _this.auth, updatedObject, originalObject, _this.config);
  });
  return Promise.all(promises);
};

// A helper to figure out what location this operation happens at.
RestWriteBatch.prototype.location = function () {
  var middle = '/classes/' + this.className + '/' + 'batch';
  return this.config.mount + middle;
};

// Returns a copy of the data and delete bad keys (_auth_data, _hashed_password...)
RestWriteBatch.prototype.sanitizedData = function (data) {
  var data = Object.keys(data).reduce(function (data, key) {
    // Regexp comes from Parse.Object.prototype.validate
    if (!/^[A-Za-z][0-9A-Za-z_]*$/.test(key)) {
      delete data[key];
    }
    return data;
  }, deepcopy(data));
  return Parse._decode(undefined, data);
};


RestWriteBatch.prototype._updateResponseWithData = function (response, data) {
  if (_lodash2.default.isEmpty(this.storage.fieldsChangedByTrigger)) {
    return response;
  }
  var clientSupportsDelete = ClientSDK.supportsForwardDelete(this.clientSDK);
  this.storage.fieldsChangedByTrigger.forEach(function (fieldName) {
    var dataValue = data[fieldName];
    var responseValue = response[fieldName];

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

exports.default = RestWriteBatch;

module.exports = RestWriteBatch;