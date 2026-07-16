"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.UsersRouter = void 0;

var _node = _interopRequireDefault(require("parse/node"));

var _Config = _interopRequireDefault(require("../Config"));

var _AccountLockout = _interopRequireDefault(require("../AccountLockout"));

var _ClassesRouter = _interopRequireDefault(require("./ClassesRouter"));

var _rest = _interopRequireDefault(require("../rest"));

var _Auth = _interopRequireDefault(require("../Auth"));

var _password = _interopRequireDefault(require("../password"));

var _triggers = require("../triggers");

var _middlewares = require("../middlewares");

var _RestWrite = _interopRequireDefault(require("../RestWrite"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) { symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); } keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

class UsersRouter extends _ClassesRouter.default {
  className() {
    return '_User';
  }
  /**
   * Removes all "_" prefixed properties from an object, except "__type"
   * @param {Object} obj An object.
   */


  static removeHiddenProperties(obj) {
    for (var key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        // Regexp comes from Parse.Object.prototype.validate
        if (key !== '__type' && !/^[A-Za-z][0-9A-Za-z_]*$/.test(key)) {
          delete obj[key];
        }
      }
    }
  }
  /**
   * After retrieving a user directly from the database, we need to remove the
   * password from the object (for security), and fix an issue some SDKs have
   * with null values
   */


  _sanitizeAuthData(user) {
    delete user.password; // Sometimes the authData still has null on that keys
    // https://github.com/parse-community/parse-server/issues/935

    if (user.authData) {
      Object.keys(user.authData).forEach(provider => {
        if (user.authData[provider] === null) {
          delete user.authData[provider];
        }
      });

      if (Object.keys(user.authData).length == 0) {
        delete user.authData;
      }
    }
  }
  /**
   * Validates a password request in login and verifyPassword
   * @param {Object} req The request
   * @returns {Object} User object
   * @private
   */


  _authenticateUserFromRequest(req) {
    return new Promise((resolve, reject) => {
      // Use query parameters instead if provided in url
      let payload = req.body;

      if (!payload.username && req.query && req.query.username || !payload.email && req.query && req.query.email) {
        payload = req.query;
      }

      const {
        username,
        email,
        password
      } = payload; // TODO: use the right error codes / descriptions.

      if (!username && !email) {
        throw new _node.default.Error(_node.default.Error.USERNAME_MISSING, 'username/email is required.');
      }

      if (!password) {
        throw new _node.default.Error(_node.default.Error.PASSWORD_MISSING, 'password is required.');
      }

      if (typeof password !== 'string' || email && typeof email !== 'string' || username && typeof username !== 'string') {
        throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'Invalid username/password.');
      }

      let user;
      let isValidPassword = false;
      let query;

      if (email && username) {
        query = {
          email,
          username
        };
      } else if (email) {
        query = {
          email
        };
      } else {
        query = {
          $or: [{
            username
          }, {
            email: username
          }]
        };
      }

      return req.config.database.find('_User', query).then(results => {
        if (!results.length) {
          throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'Invalid username/password.');
        }

        if (results.length > 1) {
          // corner case where user1 has username == user2 email
          req.config.loggerController.warn("There is a user which email is the same as another user's username, logging in based on username");
          user = results.filter(user => user.username === username)[0];
        } else {
          user = results[0];
        }

        return Promise.all([_password.default.compare(password, user.password), _password.default.compare(password, process.env.MASTER_PASSWORD_HASH)]);
      }).then(correctArray => {
        isValidPassword = correctArray[0] || correctArray[1];
        const accountLockoutPolicy = new _AccountLockout.default(user, req.config);
        return accountLockoutPolicy.handleLoginAttempt(isValidPassword);
      }).then(() => {
        if (!isValidPassword) {
          throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'Invalid username/password.');
        } // Ensure the user isn't locked out
        // A locked out user won't be able to login
        // To lock a user out, just set the ACL to `masterKey` only  ({}).
        // Empty ACL is OK


        if (!req.auth.isMaster && user.ACL && Object.keys(user.ACL).length == 0) {
          throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'Invalid username/password.');
        }

        if (req.config.verifyUserEmails && req.config.preventLoginWithUnverifiedEmail && !user.emailVerified) {
          throw new _node.default.Error(_node.default.Error.EMAIL_NOT_FOUND, 'User email is not verified.');
        }

        this._sanitizeAuthData(user);

        return resolve(user);
      }).catch(error => {
        return reject(error);
      });
    });
  }

  handleMe(req) {
    if (!req.info || !req.info.sessionToken) {
      throw new _node.default.Error(_node.default.Error.INVALID_SESSION_TOKEN, 'Invalid session token');
    }

    const sessionToken = req.info.sessionToken;
    return _rest.default.find(req.config, _Auth.default.master(req.config), '_Session', {
      sessionToken
    }, {
      include: 'user'
    }, req.info.clientSDK, req.info.context).then(response => {
      if (!response.results || response.results.length == 0 || !response.results[0].user) {
        throw new _node.default.Error(_node.default.Error.INVALID_SESSION_TOKEN, 'Invalid session token');
      } else {
        const user = response.results[0].user; // Send token back on the login, because SDKs expect that.

        user.sessionToken = sessionToken; // Remove hidden properties.

        UsersRouter.removeHiddenProperties(user);
        return {
          response: user
        };
      }
    });
  }

  async handleLogIn(req) {
    const user = await this._authenticateUserFromRequest(req); // handle password expiry policy

    if (req.config.passwordPolicy && req.config.passwordPolicy.maxPasswordAge) {
      let changedAt = user._password_changed_at;

      if (!changedAt) {
        // password was created before expiry policy was enabled.
        // simply update _User object so that it will start enforcing from now
        changedAt = new Date();
        req.config.database.update('_User', {
          username: user.username
        }, {
          _password_changed_at: _node.default._encode(changedAt)
        });
      } else {
        // check whether the password has expired
        if (changedAt.__type == 'Date') {
          changedAt = new Date(changedAt.iso);
        } // Calculate the expiry time.


        const expiresAt = new Date(changedAt.getTime() + 86400000 * req.config.passwordPolicy.maxPasswordAge);
        if (expiresAt < new Date()) // fail of current time is past password expiry time
          throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'Your password has expired. Please reset your password.');
      }
    } // Remove hidden properties.


    UsersRouter.removeHiddenProperties(user);
    req.config.filesController.expandFilesInObject(req.config, user); // Before login trigger; throws if failure

    await (0, _triggers.maybeRunTrigger)(_triggers.Types.beforeLogin, req.auth, _node.default.User.fromJSON(Object.assign({
      className: '_User'
    }, user)), null, req.config);

    const {
      sessionData,
      createSession
    } = _RestWrite.default.createSession(req.config, {
      userId: user.objectId,
      createdWith: {
        action: 'login',
        authProvider: 'password'
      },
      installationId: req.info.installationId
    });

    user.sessionToken = sessionData.sessionToken;
    await createSession();

    const afterLoginUser = _node.default.User.fromJSON(Object.assign({
      className: '_User'
    }, user));

    (0, _triggers.maybeRunTrigger)(_triggers.Types.afterLogin, _objectSpread(_objectSpread({}, req.auth), {}, {
      user: afterLoginUser
    }), afterLoginUser, null, req.config);
    return {
      response: user
    };
  }
  /**
   * This allows master-key clients to create user sessions without access to
   * user credentials. This enables systems that can authenticate access another
   * way (API key, app administrators) to act on a user's behalf.
   *
   * We create a new session rather than looking for an existing session; we
   * want this to work in situations where the user is logged out on all
   * devices, since this can be used by automated systems acting on the user's
   * behalf.
   *
   * For the moment, we're omitting event hooks and lockout checks, since
   * immediate use cases suggest /loginAs could be used for semantically
   * different reasons from /login
   */


  async handleLogInAs(req) {
    if (!req.auth.isMaster) {
      throw new _node.default.Error(_node.default.Error.OPERATION_FORBIDDEN, 'master key is required');
    }

    const userId = req.body.userId || req.query.userId;

    if (!userId) {
      throw new _node.default.Error(_node.default.Error.INVALID_VALUE, 'userId must not be empty, null, or undefined');
    }

    const queryResults = await req.config.database.find('_User', {
      objectId: userId
    });
    const user = queryResults[0];

    if (!user) {
      throw new _node.default.Error(_node.default.Error.OBJECT_NOT_FOUND, 'user not found');
    }

    this._sanitizeAuthData(user);

    const {
      sessionData,
      createSession
    } = _RestWrite.default.createSession(req.config, {
      userId,
      createdWith: {
        action: 'login',
        authProvider: 'masterkey'
      },
      installationId: req.info.installationId
    });

    user.sessionToken = sessionData.sessionToken;
    await createSession();
    return {
      response: user
    };
  }

  handleVerifyPassword(req) {
    return this._authenticateUserFromRequest(req).then(user => {
      // Remove hidden properties.
      UsersRouter.removeHiddenProperties(user);
      return {
        response: user
      };
    }).catch(error => {
      throw error;
    });
  }

  handleLogOut(req) {
    const success = {
      response: {}
    };

    if (req.info && req.info.sessionToken) {
      return _rest.default.find(req.config, _Auth.default.master(req.config), '_Session', {
        sessionToken: req.info.sessionToken
      }, undefined, req.info.clientSDK, req.info.context).then(records => {
        if (records.results && records.results.length) {
          return _rest.default.del(req.config, _Auth.default.master(req.config), '_Session', records.results[0].objectId, req.info.context).then(() => {
            this._runAfterLogoutTrigger(req, records.results[0]);

            return Promise.resolve(success);
          });
        }

        return Promise.resolve(success);
      });
    }

    return Promise.resolve(success);
  }

  _runAfterLogoutTrigger(req, session) {
    // After logout trigger
    (0, _triggers.maybeRunTrigger)(_triggers.Types.afterLogout, req.auth, _node.default.Session.fromJSON(Object.assign({
      className: '_Session'
    }, session)), null, req.config);
  }

  _throwOnBadEmailConfig(req) {
    try {
      _Config.default.validateEmailConfiguration({
        emailAdapter: req.config.userController.adapter,
        appName: req.config.appName,
        publicServerURL: req.config.publicServerURL,
        emailVerifyTokenValidityDuration: req.config.emailVerifyTokenValidityDuration,
        emailVerifyTokenReuseIfValid: req.config.emailVerifyTokenReuseIfValid
      });
    } catch (e) {
      if (typeof e === 'string') {
        // Maybe we need a Bad Configuration error, but the SDKs won't understand it. For now, Internal Server Error.
        throw new _node.default.Error(_node.default.Error.INTERNAL_SERVER_ERROR, 'An appName, publicServerURL, and emailAdapter are required for password reset and email verification functionality.');
      } else {
        throw e;
      }
    }
  }

  handleResetRequest(req) {
    this._throwOnBadEmailConfig(req);

    const {
      email,
      locale
    } = req.body;

    if (!email) {
      throw new _node.default.Error(_node.default.Error.EMAIL_MISSING, 'you must provide an email');
    }

    if (typeof email !== 'string') {
      throw new _node.default.Error(_node.default.Error.INVALID_EMAIL_ADDRESS, 'you must provide a valid email string');
    }

    const userController = req.config.userController;
    return userController.sendPasswordResetEmail(email, locale).then(() => {
      return Promise.resolve({
        response: {}
      });
    }, err => {
      if (err.code === _node.default.Error.OBJECT_NOT_FOUND) {
        // Return success so that this endpoint can't
        // be used to enumerate valid emails
        return Promise.resolve({
          response: {}
        });
      } else {
        throw err;
      }
    });
  }

  handleVerificationEmailRequest(req) {
    this._throwOnBadEmailConfig(req);

    const {
      email
    } = req.body;

    if (!email) {
      throw new _node.default.Error(_node.default.Error.EMAIL_MISSING, 'you must provide an email');
    }

    if (typeof email !== 'string') {
      throw new _node.default.Error(_node.default.Error.INVALID_EMAIL_ADDRESS, 'you must provide a valid email string');
    }

    return req.config.database.find('_User', {
      email: email
    }).then(results => {
      if (!results.length || results.length < 1) {
        throw new _node.default.Error(_node.default.Error.EMAIL_NOT_FOUND, `No user found with email ${email}`);
      }

      const user = results[0]; // remove password field, messes with saving on postgres

      delete user.password;

      if (user.emailVerified) {
        throw new _node.default.Error(_node.default.Error.OTHER_CAUSE, `Email ${email} is already verified.`);
      }

      const userController = req.config.userController;
      return userController.regenerateEmailVerifyToken(user).then(() => {
        userController.sendVerificationEmail(user);
        return {
          response: {}
        };
      });
    });
  }

  mountRoutes() {
    this.route('GET', '/users', req => {
      return this.handleFind(req);
    });
    this.route('POST', '/users', _middlewares.promiseEnsureIdempotency, req => {
      return this.handleCreate(req);
    });
    this.route('GET', '/users/me', req => {
      return this.handleMe(req);
    });
    this.route('GET', '/users/:objectId', req => {
      return this.handleGet(req);
    });
    this.route('PUT', '/users/:objectId', _middlewares.promiseEnsureIdempotency, req => {
      return this.handleUpdate(req);
    });
    this.route('DELETE', '/users/:objectId', req => {
      return this.handleDelete(req);
    });
    this.route('GET', '/login', req => {
      return this.handleLogIn(req);
    });
    this.route('POST', '/login', req => {
      return this.handleLogIn(req);
    });
    this.route('POST', '/loginAs', req => {
      return this.handleLogInAs(req);
    });
    this.route('POST', '/logout', req => {
      return this.handleLogOut(req);
    });
    this.route('POST', '/requestPasswordReset', req => {
      return this.handleResetRequest(req);
    });
    this.route('POST', '/verificationEmailRequest', req => {
      return this.handleVerificationEmailRequest(req);
    });
    this.route('GET', '/verifyPassword', req => {
      return this.handleVerifyPassword(req);
    });
  }

}

exports.UsersRouter = UsersRouter;
var _default = UsersRouter;
exports.default = _default;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIi4uLy4uL3NyYy9Sb3V0ZXJzL1VzZXJzUm91dGVyLmpzIl0sIm5hbWVzIjpbIlVzZXJzUm91dGVyIiwiQ2xhc3Nlc1JvdXRlciIsImNsYXNzTmFtZSIsInJlbW92ZUhpZGRlblByb3BlcnRpZXMiLCJvYmoiLCJrZXkiLCJPYmplY3QiLCJwcm90b3R5cGUiLCJoYXNPd25Qcm9wZXJ0eSIsImNhbGwiLCJ0ZXN0IiwiX3Nhbml0aXplQXV0aERhdGEiLCJ1c2VyIiwicGFzc3dvcmQiLCJhdXRoRGF0YSIsImtleXMiLCJmb3JFYWNoIiwicHJvdmlkZXIiLCJsZW5ndGgiLCJfYXV0aGVudGljYXRlVXNlckZyb21SZXF1ZXN0IiwicmVxIiwiUHJvbWlzZSIsInJlc29sdmUiLCJyZWplY3QiLCJwYXlsb2FkIiwiYm9keSIsInVzZXJuYW1lIiwicXVlcnkiLCJlbWFpbCIsIlBhcnNlIiwiRXJyb3IiLCJVU0VSTkFNRV9NSVNTSU5HIiwiUEFTU1dPUkRfTUlTU0lORyIsIk9CSkVDVF9OT1RfRk9VTkQiLCJpc1ZhbGlkUGFzc3dvcmQiLCIkb3IiLCJjb25maWciLCJkYXRhYmFzZSIsImZpbmQiLCJ0aGVuIiwicmVzdWx0cyIsImxvZ2dlckNvbnRyb2xsZXIiLCJ3YXJuIiwiZmlsdGVyIiwiYWxsIiwicGFzc3dvcmRDcnlwdG8iLCJjb21wYXJlIiwicHJvY2VzcyIsImVudiIsIk1BU1RFUl9QQVNTV09SRF9IQVNIIiwiY29ycmVjdEFycmF5IiwiYWNjb3VudExvY2tvdXRQb2xpY3kiLCJBY2NvdW50TG9ja291dCIsImhhbmRsZUxvZ2luQXR0ZW1wdCIsImF1dGgiLCJpc01hc3RlciIsIkFDTCIsInZlcmlmeVVzZXJFbWFpbHMiLCJwcmV2ZW50TG9naW5XaXRoVW52ZXJpZmllZEVtYWlsIiwiZW1haWxWZXJpZmllZCIsIkVNQUlMX05PVF9GT1VORCIsImNhdGNoIiwiZXJyb3IiLCJoYW5kbGVNZSIsImluZm8iLCJzZXNzaW9uVG9rZW4iLCJJTlZBTElEX1NFU1NJT05fVE9LRU4iLCJyZXN0IiwiQXV0aCIsIm1hc3RlciIsImluY2x1ZGUiLCJjbGllbnRTREsiLCJjb250ZXh0IiwicmVzcG9uc2UiLCJoYW5kbGVMb2dJbiIsInBhc3N3b3JkUG9saWN5IiwibWF4UGFzc3dvcmRBZ2UiLCJjaGFuZ2VkQXQiLCJfcGFzc3dvcmRfY2hhbmdlZF9hdCIsIkRhdGUiLCJ1cGRhdGUiLCJfZW5jb2RlIiwiX190eXBlIiwiaXNvIiwiZXhwaXJlc0F0IiwiZ2V0VGltZSIsImZpbGVzQ29udHJvbGxlciIsImV4cGFuZEZpbGVzSW5PYmplY3QiLCJUcmlnZ2VyVHlwZXMiLCJiZWZvcmVMb2dpbiIsIlVzZXIiLCJmcm9tSlNPTiIsImFzc2lnbiIsInNlc3Npb25EYXRhIiwiY3JlYXRlU2Vzc2lvbiIsIlJlc3RXcml0ZSIsInVzZXJJZCIsIm9iamVjdElkIiwiY3JlYXRlZFdpdGgiLCJhY3Rpb24iLCJhdXRoUHJvdmlkZXIiLCJpbnN0YWxsYXRpb25JZCIsImFmdGVyTG9naW5Vc2VyIiwiYWZ0ZXJMb2dpbiIsImhhbmRsZUxvZ0luQXMiLCJPUEVSQVRJT05fRk9SQklEREVOIiwiSU5WQUxJRF9WQUxVRSIsInF1ZXJ5UmVzdWx0cyIsImhhbmRsZVZlcmlmeVBhc3N3b3JkIiwiaGFuZGxlTG9nT3V0Iiwic3VjY2VzcyIsInVuZGVmaW5lZCIsInJlY29yZHMiLCJkZWwiLCJfcnVuQWZ0ZXJMb2dvdXRUcmlnZ2VyIiwic2Vzc2lvbiIsImFmdGVyTG9nb3V0IiwiU2Vzc2lvbiIsIl90aHJvd09uQmFkRW1haWxDb25maWciLCJDb25maWciLCJ2YWxpZGF0ZUVtYWlsQ29uZmlndXJhdGlvbiIsImVtYWlsQWRhcHRlciIsInVzZXJDb250cm9sbGVyIiwiYWRhcHRlciIsImFwcE5hbWUiLCJwdWJsaWNTZXJ2ZXJVUkwiLCJlbWFpbFZlcmlmeVRva2VuVmFsaWRpdHlEdXJhdGlvbiIsImVtYWlsVmVyaWZ5VG9rZW5SZXVzZUlmVmFsaWQiLCJlIiwiSU5URVJOQUxfU0VSVkVSX0VSUk9SIiwiaGFuZGxlUmVzZXRSZXF1ZXN0IiwibG9jYWxlIiwiRU1BSUxfTUlTU0lORyIsIklOVkFMSURfRU1BSUxfQUREUkVTUyIsInNlbmRQYXNzd29yZFJlc2V0RW1haWwiLCJlcnIiLCJjb2RlIiwiaGFuZGxlVmVyaWZpY2F0aW9uRW1haWxSZXF1ZXN0IiwiT1RIRVJfQ0FVU0UiLCJyZWdlbmVyYXRlRW1haWxWZXJpZnlUb2tlbiIsInNlbmRWZXJpZmljYXRpb25FbWFpbCIsIm1vdW50Um91dGVzIiwicm91dGUiLCJoYW5kbGVGaW5kIiwicHJvbWlzZUVuc3VyZUlkZW1wb3RlbmN5IiwiaGFuZGxlQ3JlYXRlIiwiaGFuZGxlR2V0IiwiaGFuZGxlVXBkYXRlIiwiaGFuZGxlRGVsZXRlIl0sIm1hcHBpbmdzIjoiOzs7Ozs7O0FBRUE7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7O0FBQ0E7Ozs7Ozs7Ozs7QUFFTyxNQUFNQSxXQUFOLFNBQTBCQyxzQkFBMUIsQ0FBd0M7QUFDN0NDLEVBQUFBLFNBQVMsR0FBRztBQUNWLFdBQU8sT0FBUDtBQUNEO0FBRUQ7QUFDRjtBQUNBO0FBQ0E7OztBQUMrQixTQUF0QkMsc0JBQXNCLENBQUNDLEdBQUQsRUFBTTtBQUNqQyxTQUFLLElBQUlDLEdBQVQsSUFBZ0JELEdBQWhCLEVBQXFCO0FBQ25CLFVBQUlFLE1BQU0sQ0FBQ0MsU0FBUCxDQUFpQkMsY0FBakIsQ0FBZ0NDLElBQWhDLENBQXFDTCxHQUFyQyxFQUEwQ0MsR0FBMUMsQ0FBSixFQUFvRDtBQUNsRDtBQUNBLFlBQUlBLEdBQUcsS0FBSyxRQUFSLElBQW9CLENBQUMsMEJBQTBCSyxJQUExQixDQUErQkwsR0FBL0IsQ0FBekIsRUFBOEQ7QUFDNUQsaUJBQU9ELEdBQUcsQ0FBQ0MsR0FBRCxDQUFWO0FBQ0Q7QUFDRjtBQUNGO0FBQ0Y7QUFFRDtBQUNGO0FBQ0E7QUFDQTtBQUNBOzs7QUFDRU0sRUFBQUEsaUJBQWlCLENBQUNDLElBQUQsRUFBTztBQUN0QixXQUFPQSxJQUFJLENBQUNDLFFBQVos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SCxHQUFHLElBQUk7QUFDbkMsYUFBTyxLQUFLeUYsWUFBTCxDQUFrQnpGLEdBQWxCLENBQVA7QUFDRCxLQUZEO0FBR0EsU0FBSzBILEtBQUwsQ0FBVyxNQUFYLEVBQW1CLHVCQUFuQixFQUE0QzFILEdBQUcsSUFBSTtBQUNqRCxhQUFPLEtBQUs4RyxrQkFBTCxDQUF3QjlHLEdBQXhCLENBQVA7QUFDRCxLQUZEO0FBR0EsU0FBSzBILEtBQUwsQ0FBVyxNQUFYLEVBQW1CLDJCQUFuQixFQUFnRDFILEdBQUcsSUFBSTtBQUNyRCxhQUFPLEtBQUtxSCw4QkFBTCxDQUFvQ3JILEdBQXBDLENBQVA7QUFDRCxLQUZEO0FBR0EsU0FBSzBILEtBQUwsQ0FBVyxLQUFYLEVBQWtCLGlCQUFsQixFQUFxQzFILEdBQUcsSUFBSTtBQUMxQyxhQUFPLEtBQUt3RixvQkFBTCxDQUEwQnhGLEdBQTFCLENBQVA7QUFDRCxLQUZEO0FBR0Q7O0FBcmU0Qzs7O2VBd2VoQ3BCLFciLCJzb3VyY2VzQ29udGVudCI6WyIvLyBUaGVzZSBtZXRob2RzIGhhbmRsZSB0aGUgVXNlci1yZWxhdGVkIHJvdXRlcy5cblxuaW1wb3J0IFBhcnNlIGZyb20gJ3BhcnNlL25vZGUnO1xuaW1wb3J0IENvbmZpZyBmcm9tICcuLi9Db25maWcnO1xuaW1wb3J0IEFjY291bnRMb2Nrb3V0IGZyb20gJy4uL0FjY291bnRMb2Nrb3V0JztcbmltcG9ydCBDbGFzc2VzUm91dGVyIGZyb20gJy4vQ2xhc3Nlc1JvdXRlcic7XG5pbXBvcnQgcmVzdCBmcm9tICcuLi9yZXN0JztcbmltcG9ydCBBdXRoIGZyb20gJy4uL0F1dGgnO1xuaW1wb3J0IHBhc3N3b3JkQ3J5cHRvIGZyb20gJy4uL3Bhc3N3b3JkJztcbmltcG9ydCB7IG1heWJlUnVuVHJpZ2dlciwgVHlwZXMgYXMgVHJpZ2dlclR5cGVzIH0gZnJvbSAnLi4vdHJpZ2dlcnMnO1xuaW1wb3J0IHsgcHJvbWlzZUVuc3VyZUlkZW1wb3RlbmN5IH0gZnJvbSAnLi4vbWlkZGxld2FyZXMnO1xuaW1wb3J0IFJlc3RXcml0ZSBmcm9tICcuLi9SZXN0V3JpdGUnO1xuXG5leHBvcnQgY2xhc3MgVXNlcnNSb3V0ZXIgZXh0ZW5kcyBDbGFzc2VzUm91dGVyIHtcbiAgY2xhc3NOYW1lKCkge1xuICAgIHJldHVybiAnX1VzZXInO1xuICB9XG5cbiAgLyoqXG4gICAqIFJlbW92ZXMgYWxsIFwiX1wiIHByZWZpeGVkIHByb3BlcnRpZXMgZnJvbSBhbiBvYmplY3QsIGV4Y2VwdCBcIl9fdHlwZVwiXG4gICAqIEBwYXJhbSB7T2JqZWN0fSBvYmogQW4gb2JqZWN0LlxuICAgKi9cbiAgc3RhdGljIHJlbW92ZUhpZGRlblByb3BlcnRpZXMob2JqKSB7XG4gICAgZm9yICh2YXIga2V5IGluIG9iaikge1xuICAgICAgaWYgKE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbChvYmosIGtleSkpIHtcbiAgICAgICAgLy8gUmVnZXhwIGNvbWVzIGZyb20gUGFyc2UuT2JqZWN0LnByb3RvdHlwZS52YWxpZGF0ZVxuICAgICAgICBpZiAoa2V5ICE9PSAnX190eXBlJyAmJiAhL15bQS1aYS16XVswLTlBLVphLXpfXSokLy50ZXN0KGtleSkpIHtcbiAgICAgICAgICBkZWxldGUgb2JqW2tleV07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQWZ0ZXIgcmV0cmlldmluZyBhIHVzZXIgZGlyZWN0bHkgZnJvbSB0aGUgZGF0YWJhc2UsIHdlIG5lZWQgdG8gcmVtb3ZlIHRoZVxuICAgKiBwYXNzd29yZCBmcm9tIHRoZSBvYmplY3QgKGZvciBzZWN1cml0eSksIGFuZCBmaXggYW4gaXNzdWUgc29tZSBTREtzIGhhdmVcbiAgICogd2l0aCBudWxsIHZhbHVlc1xuICAgKi9cbiAgX3Nhbml0aXplQXV0aERhdGEodXNlcikge1xuICAgIGRlbGV0ZSB1c2VyLnBhc3N3b3JkO1xuXG4gICAgLy8gU29tZXRpbWVzIHRoZSBhdXRoRGF0YSBzdGlsbCBoYXMgbnVsbCBvbiB0aGF0IGtleXNcbiAgICAvLyBodHRwczovL2dpdGh1Yi5jb20vcGFyc2UtY29tbXVuaXR5L3BhcnNlLXNlcnZlci9pc3N1ZXMvOTM1XG4gICAgaWYgKHVzZXIuYXV0aERhdGEpIHtcbiAgICAgIE9iamVjdC5rZXlzKHVzZXIuYXV0aERhdGEpLmZvckVhY2gocHJvdmlkZXIgPT4ge1xuICAgICAgICBpZiAodXNlci5hdXRoRGF0YVtwcm92aWRlcl0gPT09IG51bGwpIHtcbiAgICAgICAgICBkZWxldGUgdXNlci5hdXRoRGF0YVtwcm92aWRlcl07XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgaWYgKE9iamVjdC5rZXlzKHVzZXIuYXV0aERhdGEpLmxlbmd0aCA9PSAwKSB7XG4gICAgICAgIGRlbGV0ZSB1c2VyLmF1dGhEYXRhO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBWYWxpZGF0ZXMgYSBwYXNzd29yZCByZXF1ZXN0IGluIGxvZ2luIGFuZCB2ZXJpZnlQYXNzd29yZFxuICAgKiBAcGFyYW0ge09iamVjdH0gcmVxIFRoZSByZXF1ZXN0XG4gICAqIEByZXR1cm5zIHtPYmplY3R9IFVzZXIgb2JqZWN0XG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBfYXV0aGVudGljYXRlVXNlckZyb21SZXF1ZXN0KHJlcSkge1xuICAgIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG4gICAgICAvLyBVc2UgcXVlcnkgcGFyYW1ldGVycyBpbnN0ZWFkIGlmIHByb3ZpZGVkIGluIHVybFxuICAgICAgbGV0IHBheWxvYWQgPSByZXEuYm9keTtcbiAgICAgIGlmIChcbiAgICAgICAgKCFwYXlsb2FkLnVzZXJuYW1lICYmIHJlcS5xdWVyeSAmJiByZXEucXVlcnkudXNlcm5hbWUpIHx8XG4gICAgICAgICghcGF5bG9hZC5lbWFpbCAmJiByZXEucXVlcnkgJiYgcmVxLnF1ZXJ5LmVtYWlsKVxuICAgICAgKSB7XG4gICAgICAgIHBheWxvYWQgPSByZXEucXVlcnk7XG4gICAgICB9XG4gICAgICBjb25zdCB7IHVzZXJuYW1lLCBlbWFpbCwgcGFzc3dvcmQgfSA9IHBheWxvYWQ7XG5cbiAgICAgIC8vIFRPRE86IHVzZSB0aGUgcmlnaHQgZXJyb3IgY29kZXMgLyBkZXNjcmlwdGlvbnMuXG4gICAgICBpZiAoIXVzZXJuYW1lICYmICFlbWFpbCkge1xuICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuVVNFUk5BTUVfTUlTU0lORywgJ3VzZXJuYW1lL2VtYWlsIGlzIHJlcXVpcmVkLicpO1xuICAgICAgfVxuICAgICAgaWYgKCFwYXNzd29yZCkge1xuICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuUEFTU1dPUkRfTUlTU0lORywgJ3Bhc3N3b3JkIGlzIHJlcXVpcmVkLicpO1xuICAgICAgfVxuICAgICAgaWYgKFxuICAgICAgICB0eXBlb2YgcGFzc3dvcmQgIT09ICdzdHJpbmcnIHx8XG4gICAgICAgIChlbWFpbCAmJiB0eXBlb2YgZW1haWwgIT09ICdzdHJpbmcnKSB8fFxuICAgICAgICAodXNlcm5hbWUgJiYgdHlwZW9mIHVzZXJuYW1lICE9PSAnc3RyaW5nJylcbiAgICAgICkge1xuICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuT0JKRUNUX05PVF9GT1VORCwgJ0ludmFsaWQgdXNlcm5hbWUvcGFzc3dvcmQuJyk7XG4gICAgICB9XG5cbiAgICAgIGxldCB1c2VyO1xuICAgICAgbGV0IGlzVmFsaWRQYXNzd29yZCA9IGZhbHNlO1xuICAgICAgbGV0IHF1ZXJ5O1xuICAgICAgaWYgKGVtYWlsICYmIHVzZXJuYW1lKSB7XG4gICAgICAgIHF1ZXJ5ID0geyBlbWFpbCwgdXNlcm5hbWUgfTtcbiAgICAgIH0gZWxzZSBpZiAoZW1haWwpIHtcbiAgICAgICAgcXVlcnkgPSB7IGVtYWlsIH07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBxdWVyeSA9IHsgJG9yOiBbeyB1c2VybmFtZSB9LCB7IGVtYWlsOiB1c2VybmFtZSB9XSB9O1xuICAgICAgfVxuICAgICAgcmV0dXJuIHJlcS5jb25maWcuZGF0YWJhc2VcbiAgICAgICAgLmZpbmQoJ19Vc2VyJywgcXVlcnkpXG4gICAgICAgIC50aGVuKHJlc3VsdHMgPT4ge1xuICAgICAgICAgIGlmICghcmVzdWx0cy5sZW5ndGgpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBQYXJzZS5FcnJvcihQYXJzZS5FcnJvci5PQkpFQ1RfTk9UX0ZPVU5ELCAnSW52YWxpZCB1c2VybmFtZS9wYXNzd29yZC4nKTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBpZiAocmVzdWx0cy5sZW5ndGggPiAxKSB7XG4gICAgICAgICAgICAvLyBjb3JuZXIgY2FzZSB3aGVyZSB1c2VyMSBoYXMgdXNlcm5hbWUgPT0gdXNlcjIgZW1haWxcbiAgICAgICAgICAgIHJlcS5jb25maWcubG9nZ2VyQ29udHJvbGxlci53YXJuKFxuICAgICAgICAgICAgICBcIlRoZXJlIGlzIGEgdXNlciB3aGljaCBlbWFpbCBpcyB0aGUgc2FtZSBhcyBhbm90aGVyIHVzZXIncyB1c2VybmFtZSwgbG9nZ2luZyBpbiBiYXNlZCBvbiB1c2VybmFtZVwiXG4gICAgICAgICAgICApO1xuICAgICAgICAgICAgdXNlciA9IHJlc3VsdHMuZmlsdGVyKHVzZXIgPT4gdXNlci51c2VybmFtZSA9PT0gdXNlcm5hbWUpWzBdO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICB1c2VyID0gcmVzdWx0c1swXTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICByZXR1cm4gUHJvbWlzZS5hbGwoW1xuICAgICAgICAgICAgICBwYXNzd29yZENyeXB0by5jb21wYXJlKHBhc3N3b3JkLCB1c2VyLnBhc3N3b3JkKSxcbiAgICAgICAgICAgICAgcGFzc3dvcmRDcnlwdG8uY29tcGFyZShwYXNzd29yZCwgcHJvY2Vzcy5lbnYuTUFTVEVSX1BBU1NXT1JEX0hBU0gpXG4gICAgICAgICAgXSk7XG4gICAgICAgIH0pXG4gICAgICAgIC50aGVuKChjb3JyZWN0QXJyYXkpID0+IHtcbiAgICAgICAgICAgICAgaXNWYWxpZFBhc3N3b3JkID0gY29ycmVjdEFycmF5WzBdIHx8IGNvcnJlY3RBcnJheVsxXTtcbiAgICAgICAgICAgICAgY29uc3QgYWNjb3VudExvY2tvdXRQb2xpY3kgPSBuZXcgQWNjb3VudExvY2tvdXQodXNlciwgcmVxLmNvbmZpZyk7XG4gICAgICAgICAgICAgIHJldHVybiBhY2NvdW50TG9ja291dFBvbGljeS5oYW5kbGVMb2dpbkF0dGVtcHQoaXNWYWxpZFBhc3N3b3JkKTtcbiAgICAgICAgfSlcbiAgICAgICAgLnRoZW4oKCkgPT4ge1xuICAgICAgICAgIGlmICghaXNWYWxpZFBhc3N3b3JkKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuT0JKRUNUX05PVF9GT1VORCwgJ0ludmFsaWQgdXNlcm5hbWUvcGFzc3dvcmQuJyk7XG4gICAgICAgICAgfVxuICAgICAgICAgIC8vIEVuc3VyZSB0aGUgdXNlciBpc24ndCBsb2NrZWQgb3V0XG4gICAgICAgICAgLy8gQSBsb2NrZWQgb3V0IHVzZXIgd29uJ3QgYmUgYWJsZSB0byBsb2dpblxuICAgICAgICAgIC8vIFRvIGxvY2sgYSB1c2VyIG91dCwganVzdCBzZXQgdGhlIEFDTCB0byBgbWFzdGVyS2V5YCBvbmx5ICAoe30pLlxuICAgICAgICAgIC8vIEVtcHR5IEFDTCBpcyBPS1xuICAgICAgICAgIGlmICghcmVxLmF1dGguaXNNYXN0ZXIgJiYgdXNlci5BQ0wgJiYgT2JqZWN0LmtleXModXNlci5BQ0wpLmxlbmd0aCA9PSAwKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuT0JKRUNUX05PVF9GT1VORCwgJ0ludmFsaWQgdXNlcm5hbWUvcGFzc3dvcmQuJyk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChcbiAgICAgICAgICAgIHJlcS5jb25maWcudmVyaWZ5VXNlckVtYWlscyAmJlxuICAgICAgICAgICAgcmVxLmNvbmZpZy5wcmV2ZW50TG9naW5XaXRoVW52ZXJpZmllZEVtYWlsICYmXG4gICAgICAgICAgICAhdXNlci5lbWFpbFZlcmlmaWVkXG4gICAgICAgICAgKSB7XG4gICAgICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuRU1BSUxfTk9UX0ZPVU5ELCAnVXNlciBlbWFpbCBpcyBub3QgdmVyaWZpZWQuJyk7XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgdGhpcy5fc2FuaXRpemVBdXRoRGF0YSh1c2VyKTtcblxuICAgICAgICAgIHJldHVybiByZXNvbHZlKHVzZXIpO1xuICAgICAgICB9KVxuICAgICAgICAuY2F0Y2goZXJyb3IgPT4ge1xuICAgICAgICAgIHJldHVybiByZWplY3QoZXJyb3IpO1xuICAgICAgICB9KTtcbiAgICB9KTtcbiAgfVxuXG4gIGhhbmRsZU1lKHJlcSkge1xuICAgIGlmICghcmVxLmluZm8gfHwgIXJlcS5pbmZvLnNlc3Npb25Ub2tlbikge1xuICAgICAgdGhyb3cgbmV3IFBhcnNlLkVycm9yKFBhcnNlLkVycm9yLklOVkFMSURfU0VTU0lPTl9UT0tFTiwgJ0ludmFsaWQgc2Vzc2lvbiB0b2tlbicpO1xuICAgIH1cbiAgICBjb25zdCBzZXNzaW9uVG9rZW4gPSByZXEuaW5mby5zZXNzaW9uVG9rZW47XG4gICAgcmV0dXJuIHJlc3RcbiAgICAgIC5maW5kKFxuICAgICAgICByZXEuY29uZmlnLFxuICAgICAgICBBdXRoLm1hc3RlcihyZXEuY29uZmlnKSxcbiAgICAgICAgJ19TZXNzaW9uJyxcbiAgICAgICAgeyBzZXNzaW9uVG9rZW4gfSxcbiAgICAgICAgeyBpbmNsdWRlOiAndXNlcicgfSxcbiAgICAgICAgcmVxLmluZm8uY2xpZW50U0RLLFxuICAgICAgICByZXEuaW5mby5jb250ZXh0XG4gICAgICApXG4gICAgICAudGhlbihyZXNwb25zZSA9PiB7XG4gICAgICAgIGlmICghcmVzcG9uc2UucmVzdWx0cyB8fCByZXNwb25zZS5yZXN1bHRzLmxlbmd0aCA9PSAwIHx8ICFyZXNwb25zZS5yZXN1bHRzWzBdLnVzZXIpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuSU5WQUxJRF9TRVNTSU9OX1RPS0VOLCAnSW52YWxpZCBzZXNzaW9uIHRva2VuJyk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc3QgdXNlciA9IHJlc3BvbnNlLnJlc3VsdHNbMF0udXNlcjtcbiAgICAgICAgICAvLyBTZW5kIHRva2VuIGJhY2sgb24gdGhlIGxvZ2luLCBiZWNhdXNlIFNES3MgZXhwZWN0IHRoYXQuXG4gICAgICAgICAgdXNlci5zZXNzaW9uVG9rZW4gPSBzZXNzaW9uVG9rZW47XG5cbiAgICAgICAgICAvLyBSZW1vdmUgaGlkZGVuIHByb3BlcnRpZXMuXG4gICAgICAgICAgVXNlcnNSb3V0ZXIucmVtb3ZlSGlkZGVuUHJvcGVydGllcyh1c2VyKTtcblxuICAgICAgICAgIHJldHVybiB7IHJlc3BvbnNlOiB1c2VyIH07XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICB9XG5cbiAgYXN5bmMgaGFuZGxlTG9nSW4ocmVxKSB7XG4gICAgY29uc3QgdXNlciA9IGF3YWl0IHRoaXMuX2F1dGhlbnRpY2F0ZVVzZXJGcm9tUmVxdWVzdChyZXEpO1xuXG4gICAgLy8gaGFuZGxlIHBhc3N3b3JkIGV4cGlyeSBwb2xpY3lcbiAgICBpZiAocmVxLmNvbmZpZy5wYXNzd29yZFBvbGljeSAmJiByZXEuY29uZmlnLnBhc3N3b3JkUG9saWN5Lm1heFBhc3N3b3JkQWdlKSB7XG4gICAgICBsZXQgY2hhbmdlZEF0ID0gdXNlci5fcGFzc3dvcmRfY2hhbmdlZF9hdDtcblxuICAgICAgaWYgKCFjaGFuZ2VkQXQpIHtcbiAgICAgICAgLy8gcGFzc3dvcmQgd2FzIGNyZWF0ZWQgYmVmb3JlIGV4cGlyeSBwb2xpY3kgd2FzIGVuYWJsZWQuXG4gICAgICAgIC8vIHNpbXBseSB1cGRhdGUgX1VzZXIgb2JqZWN0IHNvIHRoYXQgaXQgd2lsbCBzdGFydCBlbmZvcmNpbmcgZnJvbSBub3dcbiAgICAgICAgY2hhbmdlZEF0ID0gbmV3IERhdGUoKTtcbiAgICAgICAgcmVxLmNvbmZpZy5kYXRhYmFzZS51cGRhdGUoXG4gICAgICAgICAgJ19Vc2VyJyxcbiAgICAgICAgICB7IHVzZXJuYW1lOiB1c2VyLnVzZXJuYW1lIH0sXG4gICAgICAgICAgeyBfcGFzc3dvcmRfY2hhbmdlZF9hdDogUGFyc2UuX2VuY29kZShjaGFuZ2VkQXQpIH1cbiAgICAgICAgKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIGNoZWNrIHdoZXRoZXIgdGhlIHBhc3N3b3JkIGhhcyBleHBpcmVkXG4gICAgICAgIGlmIChjaGFuZ2VkQXQuX190eXBlID09ICdEYXRlJykge1xuICAgICAgICAgIGNoYW5nZWRBdCA9IG5ldyBEYXRlKGNoYW5nZWRBdC5pc28pO1xuICAgICAgICB9XG4gICAgICAgIC8vIENhbGN1bGF0ZSB0aGUgZXhwaXJ5IHRpbWUuXG4gICAgICAgIGNvbnN0IGV4cGlyZXNBdCA9IG5ldyBEYXRlKFxuICAgICAgICAgIGNoYW5nZWRBdC5nZXRUaW1lKCkgKyA4NjQwMDAwMCAqIHJlcS5jb25maWcucGFzc3dvcmRQb2xpY3kubWF4UGFzc3dvcmRBZ2VcbiAgICAgICAgKTtcbiAgICAgICAgaWYgKGV4cGlyZXNBdCA8IG5ldyBEYXRlKCkpXG4gICAgICAgICAgLy8gZmFpbCBvZiBjdXJyZW50IHRpbWUgaXMgcGFzdCBwYXNzd29yZCBleHBpcnkgdGltZVxuICAgICAgICAgIHRocm93IG5ldyBQYXJzZS5FcnJvcihcbiAgICAgICAgICAgIFBhcnNlLkVycm9yLk9CSkVDVF9OT1RfRk9VTkQsXG4gICAgICAgICAgICAnWW91ciBwYXNzd29yZCBoYXMgZXhwaXJlZC4gUGxlYXNlIHJlc2V0IHlvdXIgcGFzc3dvcmQuJ1xuICAgICAgICAgICk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gUmVtb3ZlIGhpZGRlbiBwcm9wZXJ0aWVzLlxuICAgIFVzZXJzUm91dGVyLnJlbW92ZUhpZGRlblByb3BlcnRpZXModXNlcik7XG5cbiAgICByZXEuY29uZmlnLmZpbGVzQ29udHJvbGxlci5leHBhbmRGaWxlc0luT2JqZWN0KHJlcS5jb25maWcsIHVzZXIpO1xuXG4gICAgLy8gQmVmb3JlIGxvZ2luIHRyaWdnZXI7IHRocm93cyBpZiBmYWlsdXJlXG4gICAgYXdhaXQgbWF5YmVSdW5UcmlnZ2VyKFxuICAgICAgVHJpZ2dlclR5cGVzLmJlZm9yZUxvZ2luLFxuICAgICAgcmVxLmF1dGgsXG4gICAgICBQYXJzZS5Vc2VyLmZyb21KU09OKE9iamVjdC5hc3NpZ24oeyBjbGFzc05hbWU6ICdfVXNlcicgfSwgdXNlcikpLFxuICAgICAgbnVsbCxcbiAgICAgIHJlcS5jb25maWdcbiAgICApO1xuXG4gICAgY29uc3QgeyBzZXNzaW9uRGF0YSwgY3JlYXRlU2Vzc2lvbiB9ID0gUmVzdFdyaXRlLmNyZWF0ZVNlc3Npb24ocmVxLmNvbmZpZywge1xuICAgICAgdXNlcklkOiB1c2VyLm9iamVjdElkLFxuICAgICAgY3JlYXRlZFdpdGg6IHtcbiAgICAgICAgYWN0aW9uOiAnbG9naW4nLFxuICAgICAgICBhdXRoUHJvdmlkZXI6ICdwYXNzd29yZCcsXG4gICAgICB9LFxuICAgICAgaW5zdGFsbGF0aW9uSWQ6IHJlcS5pbmZvLmluc3RhbGxhdGlvbklkLFxuICAgIH0pO1xuXG4gICAgdXNlci5zZXNzaW9uVG9rZW4gPSBzZXNzaW9uRGF0YS5zZXNzaW9uVG9rZW47XG5cbiAgICBhd2FpdCBjcmVhdGVTZXNzaW9uKCk7XG5cbiAgICBjb25zdCBhZnRlckxvZ2luVXNlciA9IFBhcnNlLlVzZXIuZnJvbUpTT04oT2JqZWN0LmFzc2lnbih7IGNsYXNzTmFtZTogJ19Vc2VyJyB9LCB1c2VyKSk7XG4gICAgbWF5YmVSdW5UcmlnZ2VyKFxuICAgICAgVHJpZ2dlclR5cGVzLmFmdGVyTG9naW4sXG4gICAgICB7IC4uLnJlcS5hdXRoLCB1c2VyOiBhZnRlckxvZ2luVXNlciB9LFxuICAgICAgYWZ0ZXJMb2dpblVzZXIsXG4gICAgICBudWxsLFxuICAgICAgcmVxLmNvbmZpZ1xuICAgICk7XG5cbiAgICByZXR1cm4geyByZXNwb25zZTogdXNlciB9O1xuICB9XG5cbiAgLyoqXG4gICAqIFRoaXMgYWxsb3dzIG1hc3Rlci1rZXkgY2xpZW50cyB0byBjcmVhdGUgdXNlciBzZXNzaW9ucyB3aXRob3V0IGFjY2VzcyB0b1xuICAgKiB1c2VyIGNyZWRlbnRpYWxzLiBUaGlzIGVuYWJsZXMgc3lzdGVtcyB0aGF0IGNhbiBhdXRoZW50aWNhdGUgYWNjZXNzIGFub3RoZXJcbiAgICogd2F5IChBUEkga2V5LCBhcHAgYWRtaW5pc3RyYXRvcnMpIHRvIGFjdCBvbiBhIHVzZXIncyBiZWhhbGYuXG4gICAqXG4gICAqIFdlIGNyZWF0ZSBhIG5ldyBzZXNzaW9uIHJhdGhlciB0aGFuIGxvb2tpbmcgZm9yIGFuIGV4aXN0aW5nIHNlc3Npb247IHdlXG4gICAqIHdhbnQgdGhpcyB0byB3b3JrIGluIHNpdHVhdGlvbnMgd2hlcmUgdGhlIHVzZXIgaXMgbG9nZ2VkIG91dCBvbiBhbGxcbiAgICogZGV2aWNlcywgc2luY2UgdGhpcyBjYW4gYmUgdXNlZCBieSBhdXRvbWF0ZWQgc3lzdGVtcyBhY3Rpbmcgb24gdGhlIHVzZXInc1xuICAgKiBiZWhhbGYuXG4gICAqXG4gICAqIEZvciB0aGUgbW9tZW50LCB3ZSdyZSBvbWl0dGluZyBldmVudCBob29rcyBhbmQgbG9ja291dCBjaGVja3MsIHNpbmNlXG4gICAqIGltbWVkaWF0ZSB1c2UgY2FzZXMgc3VnZ2VzdCAvbG9naW5BcyBjb3VsZCBiZSB1c2VkIGZvciBzZW1hbnRpY2FsbHlcbiAgICogZGlmZmVyZW50IHJlYXNvbnMgZnJvbSAvbG9naW5cbiAgICovXG4gIGFzeW5jIGhhbmRsZUxvZ0luQXMocmVxKSB7XG4gICAgaWYgKCFyZXEuYXV0aC5pc01hc3Rlcikge1xuICAgICAgdGhyb3cgbmV3IFBhcnNlLkVycm9yKFBhcnNlLkVycm9yLk9QRVJBVElPTl9GT1JCSURERU4sICdtYXN0ZXIga2V5IGlzIHJlcXVpcmVkJyk7XG4gICAgfVxuXG4gICAgY29uc3QgdXNlcklkID0gcmVxLmJvZHkudXNlcklkIHx8IHJlcS5xdWVyeS51c2VySWQ7XG4gICAgaWYgKCF1c2VySWQpIHtcbiAgICAgIHRocm93IG5ldyBQYXJzZS5FcnJvcihcbiAgICAgICAgUGFyc2UuRXJyb3IuSU5WQUxJRF9WQUxVRSxcbiAgICAgICAgJ3VzZXJJZCBtdXN0IG5vdCBiZSBlbXB0eSwgbnVsbCwgb3IgdW5kZWZpbmVkJ1xuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeVJlc3VsdHMgPSBhd2FpdCByZXEuY29uZmlnLmRhdGFiYXNlLmZpbmQoJ19Vc2VyJywgeyBvYmplY3RJZDogdXNlcklkIH0pO1xuICAgIGNvbnN0IHVzZXIgPSBxdWVyeVJlc3VsdHNbMF07XG4gICAgaWYgKCF1c2VyKSB7XG4gICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoUGFyc2UuRXJyb3IuT0JKRUNUX05PVF9GT1VORCwgJ3VzZXIgbm90IGZvdW5kJyk7XG4gICAgfVxuXG4gICAgdGhpcy5fc2FuaXRpemVBdXRoRGF0YSh1c2VyKTtcblxuICAgIGNvbnN0IHsgc2Vzc2lvbkRhdGEsIGNyZWF0ZVNlc3Npb24gfSA9IFJlc3RXcml0ZS5jcmVhdGVTZXNzaW9uKHJlcS5jb25maWcsIHtcbiAgICAgIHVzZXJJZCxcbiAgICAgIGNyZWF0ZWRXaXRoOiB7XG4gICAgICAgIGFjdGlvbjogJ2xvZ2luJyxcbiAgICAgICAgYXV0aFByb3ZpZGVyOiAnbWFzdGVya2V5JyxcbiAgICAgIH0sXG4gICAgICBpbnN0YWxsYXRpb25JZDogcmVxLmluZm8uaW5zdGFsbGF0aW9uSWQsXG4gICAgfSk7XG5cbiAgICB1c2VyLnNlc3Npb25Ub2tlbiA9IHNlc3Npb25EYXRhLnNlc3Npb25Ub2tlbjtcblxuICAgIGF3YWl0IGNyZWF0ZVNlc3Npb24oKTtcblxuICAgIHJldHVybiB7IHJlc3BvbnNlOiB1c2VyIH07XG4gIH1cblxuICBoYW5kbGVWZXJpZnlQYXNzd29yZChyZXEpIHtcbiAgICByZXR1cm4gdGhpcy5fYXV0aGVudGljYXRlVXNlckZyb21SZXF1ZXN0KHJlcSlcbiAgICAgIC50aGVuKHVzZXIgPT4ge1xuICAgICAgICAvLyBSZW1vdmUgaGlkZGVuIHByb3BlcnRpZXMuXG4gICAgICAgIFVzZXJzUm91dGVyLnJlbW92ZUhpZGRlblByb3BlcnRpZXModXNlcik7XG5cbiAgICAgICAgcmV0dXJuIHsgcmVzcG9uc2U6IHVzZXIgfTtcbiAgICAgIH0pXG4gICAgICAuY2F0Y2goZXJyb3IgPT4ge1xuICAgICAgICB0aHJvdyBlcnJvcjtcbiAgICAgIH0pO1xuICB9XG5cbiAgaGFuZGxlTG9nT3V0KHJlcSkge1xuICAgIGNvbnN0IHN1Y2Nlc3MgPSB7IHJlc3BvbnNlOiB7fSB9O1xuICAgIGlmIChyZXEuaW5mbyAmJiByZXEuaW5mby5zZXNzaW9uVG9rZW4pIHtcbiAgICAgIHJldHVybiByZXN0XG4gICAgICAgIC5maW5kKFxuICAgICAgICAgIHJlcS5jb25maWcsXG4gICAgICAgICAgQXV0aC5tYXN0ZXIocmVxLmNvbmZpZyksXG4gICAgICAgICAgJ19TZXNzaW9uJyxcbiAgICAgICAgICB7IHNlc3Npb25Ub2tlbjogcmVxLmluZm8uc2Vzc2lvblRva2VuIH0sXG4gICAgICAgICAgdW5kZWZpbmVkLFxuICAgICAgICAgIHJlcS5pbmZvLmNsaWVudFNESyxcbiAgICAgICAgICByZXEuaW5mby5jb250ZXh0XG4gICAgICAgIClcbiAgICAgICAgLnRoZW4ocmVjb3JkcyA9PiB7XG4gICAgICAgICAgaWYgKHJlY29yZHMucmVzdWx0cyAmJiByZWNvcmRzLnJlc3VsdHMubGVuZ3RoKSB7XG4gICAgICAgICAgICByZXR1cm4gcmVzdFxuICAgICAgICAgICAgICAuZGVsKFxuICAgICAgICAgICAgICAgIHJlcS5jb25maWcsXG4gICAgICAgICAgICAgICAgQXV0aC5tYXN0ZXIocmVxLmNvbmZpZyksXG4gICAgICAgICAgICAgICAgJ19TZXNzaW9uJyxcbiAgICAgICAgICAgICAgICByZWNvcmRzLnJlc3VsdHNbMF0ub2JqZWN0SWQsXG4gICAgICAgICAgICAgICAgcmVxLmluZm8uY29udGV4dFxuICAgICAgICAgICAgICApXG4gICAgICAgICAgICAgIC50aGVuKCgpID0+IHtcbiAgICAgICAgICAgICAgICB0aGlzLl9ydW5BZnRlckxvZ291dFRyaWdnZXIocmVxLCByZWNvcmRzLnJlc3VsdHNbMF0pO1xuICAgICAgICAgICAgICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUoc3VjY2Vzcyk7XG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKHN1Y2Nlc3MpO1xuICAgICAgICB9KTtcbiAgICB9XG4gICAgcmV0dXJuIFByb21pc2UucmVzb2x2ZShzdWNjZXNzKTtcbiAgfVxuXG4gIF9ydW5BZnRlckxvZ291dFRyaWdnZXIocmVxLCBzZXNzaW9uKSB7XG4gICAgLy8gQWZ0ZXIgbG9nb3V0IHRyaWdnZXJcbiAgICBtYXliZVJ1blRyaWdnZXIoXG4gICAgICBUcmlnZ2VyVHlwZXMuYWZ0ZXJMb2dvdXQsXG4gICAgICByZXEuYXV0aCxcbiAgICAgIFBhcnNlLlNlc3Npb24uZnJvbUpTT04oT2JqZWN0LmFzc2lnbih7IGNsYXNzTmFtZTogJ19TZXNzaW9uJyB9LCBzZXNzaW9uKSksXG4gICAgICBudWxsLFxuICAgICAgcmVxLmNvbmZpZ1xuICAgICk7XG4gIH1cblxuICBfdGhyb3dPbkJhZEVtYWlsQ29uZmlnKHJlcSkge1xuICAgIHRyeSB7XG4gICAgICBDb25maWcudmFsaWRhdGVFbWFpbENvbmZpZ3VyYXRpb24oe1xuICAgICAgICBlbWFpbEFkYXB0ZXI6IHJlcS5jb25maWcudXNlckNvbnRyb2xsZXIuYWRhcHRlcixcbiAgICAgICAgYXBwTmFtZTogcmVxLmNvbmZpZy5hcHBOYW1lLFxuICAgICAgICBwdWJsaWNTZXJ2ZXJVUkw6IHJlcS5jb25maWcucHVibGljU2VydmVyVVJMLFxuICAgICAgICBlbWFpbFZlcmlmeVRva2VuVmFsaWRpdHlEdXJhdGlvbjogcmVxLmNvbmZpZy5lbWFpbFZlcmlmeVRva2VuVmFsaWRpdHlEdXJhdGlvbixcbiAgICAgICAgZW1haWxWZXJpZnlUb2tlblJldXNlSWZWYWxpZDogcmVxLmNvbmZpZy5lbWFpbFZlcmlmeVRva2VuUmV1c2VJZlZhbGlkLFxuICAgICAgfSk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgaWYgKHR5cGVvZiBlID09PSAnc3RyaW5nJykge1xuICAgICAgICAvLyBNYXliZSB3ZSBuZWVkIGEgQmFkIENvbmZpZ3VyYXRpb24gZXJyb3IsIGJ1dCB0aGUgU0RLcyB3b24ndCB1bmRlcnN0YW5kIGl0LiBGb3Igbm93LCBJbnRlcm5hbCBTZXJ2ZXIgRXJyb3IuXG4gICAgICAgIHRocm93IG5ldyBQYXJzZS5FcnJvcihcbiAgICAgICAgICBQYXJzZS5FcnJvci5JTlRFUk5BTF9TRVJWRVJfRVJST1IsXG4gICAgICAgICAgJ0FuIGFwcE5hbWUsIHB1YmxpY1NlcnZlclVSTCwgYW5kIGVtYWlsQWRhcHRlciBhcmUgcmVxdWlyZWQgZm9yIHBhc3N3b3JkIHJlc2V0IGFuZCBlbWFpbCB2ZXJpZmljYXRpb24gZnVuY3Rpb25hbGl0eS4nXG4gICAgICAgICk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBlO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGhhbmRsZVJlc2V0UmVxdWVzdChyZXEpIHtcbiAgICB0aGlzLl90aHJvd09uQmFkRW1haWxDb25maWcocmVxKTtcblxuICAgIGNvbnN0IHsgZW1haWwsIGxvY2FsZSB9ID0gcmVxLmJvZHk7XG4gICAgaWYgKCFlbWFpbCkge1xuICAgICAgdGhyb3cgbmV3IFBhcnNlLkVycm9yKFBhcnNlLkVycm9yLkVNQUlMX01JU1NJTkcsICd5b3UgbXVzdCBwcm92aWRlIGFuIGVtYWlsJyk7XG4gICAgfVxuICAgIGlmICh0eXBlb2YgZW1haWwgIT09ICdzdHJpbmcnKSB7XG4gICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoXG4gICAgICAgIFBhcnNlLkVycm9yLklOVkFMSURfRU1BSUxfQUREUkVTUyxcbiAgICAgICAgJ3lvdSBtdXN0IHByb3ZpZGUgYSB2YWxpZCBlbWFpbCBzdHJpbmcnXG4gICAgICApO1xuICAgIH1cbiAgICBjb25zdCB1c2VyQ29udHJvbGxlciA9IHJlcS5jb25maWcudXNlckNvbnRyb2xsZXI7XG4gICAgcmV0dXJuIHVzZXJDb250cm9sbGVyLnNlbmRQYXNzd29yZFJlc2V0RW1haWwoZW1haWwsIGxvY2FsZSkudGhlbihcbiAgICAgICgpID0+IHtcbiAgICAgICAgcmV0dXJuIFByb21pc2UucmVzb2x2ZSh7XG4gICAgICAgICAgcmVzcG9uc2U6IHt9LFxuICAgICAgICB9KTtcbiAgICAgIH0sXG4gICAgICBlcnIgPT4ge1xuICAgICAgICBpZiAoZXJyLmNvZGUgPT09IFBhcnNlLkVycm9yLk9CSkVDVF9OT1RfRk9VTkQpIHtcbiAgICAgICAgICAvLyBSZXR1cm4gc3VjY2VzcyBzbyB0aGF0IHRoaXMgZW5kcG9pbnQgY2FuJ3RcbiAgICAgICAgICAvLyBiZSB1c2VkIHRvIGVudW1lcmF0ZSB2YWxpZCBlbWFpbHNcbiAgICAgICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKHtcbiAgICAgICAgICAgIHJlc3BvbnNlOiB7fSxcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aHJvdyBlcnI7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICApO1xuICB9XG5cbiAgaGFuZGxlVmVyaWZpY2F0aW9uRW1haWxSZXF1ZXN0KHJlcSkge1xuICAgIHRoaXMuX3Rocm93T25CYWRFbWFpbENvbmZpZyhyZXEpO1xuXG4gICAgY29uc3QgeyBlbWFpbCB9ID0gcmVxLmJvZHk7XG4gICAgaWYgKCFlbWFpbCkge1xuICAgICAgdGhyb3cgbmV3IFBhcnNlLkVycm9yKFBhcnNlLkVycm9yLkVNQUlMX01JU1NJTkcsICd5b3UgbXVzdCBwcm92aWRlIGFuIGVtYWlsJyk7XG4gICAgfVxuICAgIGlmICh0eXBlb2YgZW1haWwgIT09ICdzdHJpbmcnKSB7XG4gICAgICB0aHJvdyBuZXcgUGFyc2UuRXJyb3IoXG4gICAgICAgIFBhcnNlLkVycm9yLklOVkFMSURfRU1BSUxfQUREUkVTUyxcbiAgICAgICAgJ3lvdSBtdXN0IHByb3ZpZGUgYSB2YWxpZCBlbWFpbCBzdHJpbmcnXG4gICAgICApO1xuICAgIH1cblxuICAgIHJldHVybiByZXEuY29uZmlnLmRhdGFiYXNlLmZpbmQoJ19Vc2VyJywgeyBlbWFpbDogZW1haWwgfSkudGhlbihyZXN1bHRzID0+IHtcbiAgICAgIGlmICghcmVzdWx0cy5sZW5ndGggfHwgcmVzdWx0cy5sZW5ndGggPCAxKSB7XG4gICAgICAgIHRocm93IG5ldyBQYXJzZS5FcnJvcihQYXJzZS5FcnJvci5FTUFJTF9OT1RfRk9VTkQsIGBObyB1c2VyIGZvdW5kIHdpdGggZW1haWwgJHtlbWFpbH1gKTtcbiAgICAgIH1cbiAgICAgIGNvbnN0IHVzZXIgPSByZXN1bHRzWzBdO1xuXG4gICAgICAvLyByZW1vdmUgcGFzc3dvcmQgZmllbGQsIG1lc3NlcyB3aXRoIHNhdmluZyBvbiBwb3N0Z3Jlc1xuICAgICAgZGVsZXRlIHVzZXIucGFzc3dvcmQ7XG5cbiAgICAgIGlmICh1c2VyLmVtYWlsVmVyaWZpZWQpIHtcbiAgICAgICAgdGhyb3cgbmV3IFBhcnNlLkVycm9yKFBhcnNlLkVycm9yLk9USEVSX0NBVVNFLCBgRW1haWwgJHtlbWFpbH0gaXMgYWxyZWFkeSB2ZXJpZmllZC5gKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgdXNlckNvbnRyb2xsZXIgPSByZXEuY29uZmlnLnVzZXJDb250cm9sbGVyO1xuICAgICAgcmV0dXJuIHVzZXJDb250cm9sbGVyLnJlZ2VuZXJhdGVFbWFpbFZlcmlmeVRva2VuKHVzZXIpLnRoZW4oKCkgPT4ge1xuICAgICAgICB1c2VyQ29udHJvbGxlci5zZW5kVmVyaWZpY2F0aW9uRW1haWwodXNlcik7XG4gICAgICAgIHJldHVybiB7IHJlc3BvbnNlOiB7fSB9O1xuICAgICAgfSk7XG4gICAgfSk7XG4gIH1cblxuICBtb3VudFJvdXRlcygpIHtcbiAgICB0aGlzLnJvdXRlKCdHRVQnLCAnL3VzZXJzJywgcmVxID0+IHtcbiAgICAgIHJldHVybiB0aGlzLmhhbmRsZUZpbmQocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdQT1NUJywgJy91c2VycycsIHByb21pc2VFbnN1cmVJZGVtcG90ZW5jeSwgcmVxID0+IHtcbiAgICAgIHJldHVybiB0aGlzLmhhbmRsZUNyZWF0ZShyZXEpO1xuICAgIH0pO1xuICAgIHRoaXMucm91dGUoJ0dFVCcsICcvdXNlcnMvbWUnLCByZXEgPT4ge1xuICAgICAgcmV0dXJuIHRoaXMuaGFuZGxlTWUocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdHRVQnLCAnL3VzZXJzLzpvYmplY3RJZCcsIHJlcSA9PiB7XG4gICAgICByZXR1cm4gdGhpcy5oYW5kbGVHZXQocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdQVVQnLCAnL3VzZXJzLzpvYmplY3RJZCcsIHByb21pc2VFbnN1cmVJZGVtcG90ZW5jeSwgcmVxID0+IHtcbiAgICAgIHJldHVybiB0aGlzLmhhbmRsZVVwZGF0ZShyZXEpO1xuICAgIH0pO1xuICAgIHRoaXMucm91dGUoJ0RFTEVURScsICcvdXNlcnMvOm9iamVjdElkJywgcmVxID0+IHtcbiAgICAgIHJldHVybiB0aGlzLmhhbmRsZURlbGV0ZShyZXEpO1xuICAgIH0pO1xuICAgIHRoaXMucm91dGUoJ0dFVCcsICcvbG9naW4nLCByZXEgPT4ge1xuICAgICAgcmV0dXJuIHRoaXMuaGFuZGxlTG9nSW4ocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdQT1NUJywgJy9sb2dpbicsIHJlcSA9PiB7XG4gICAgICByZXR1cm4gdGhpcy5oYW5kbGVMb2dJbihyZXEpO1xuICAgIH0pO1xuICAgIHRoaXMucm91dGUoJ1BPU1QnLCAnL2xvZ2luQXMnLCByZXEgPT4ge1xuICAgICAgcmV0dXJuIHRoaXMuaGFuZGxlTG9nSW5BcyhyZXEpO1xuICAgIH0pO1xuICAgIHRoaXMucm91dGUoJ1BPU1QnLCAnL2xvZ291dCcsIHJlcSA9PiB7XG4gICAgICByZXR1cm4gdGhpcy5oYW5kbGVMb2dPdXQocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdQT1NUJywgJy9yZXF1ZXN0UGFzc3dvcmRSZXNldCcsIHJlcSA9PiB7XG4gICAgICByZXR1cm4gdGhpcy5oYW5kbGVSZXNldFJlcXVlc3QocmVxKTtcbiAgICB9KTtcbiAgICB0aGlzLnJvdXRlKCdQT1NUJywgJy92ZXJpZmljYXRpb25FbWFpbFJlcXVlc3QnLCByZXEgPT4ge1xuICAgICAgcmV0dXJuIHRoaXMuaGFuZGxlVmVyaWZpY2F0aW9uRW1haWxSZXF1ZXN0KHJlcSk7XG4gICAgfSk7XG4gICAgdGhpcy5yb3V0ZSgnR0VUJywgJy92ZXJpZnlQYXNzd29yZCcsIHJlcSA9PiB7XG4gICAgICByZXR1cm4gdGhpcy5oYW5kbGVWZXJpZnlQYXNzd29yZChyZXEpO1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBkZWZhdWx0IFVzZXJzUm91dGVyO1xuIl19