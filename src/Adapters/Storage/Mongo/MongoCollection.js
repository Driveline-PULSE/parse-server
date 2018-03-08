const mongodb = require('mongodb');
const Collection = mongodb.Collection;

export default class MongoCollection {
  _mongoCollection:Collection;

  constructor(mongoCollection:Collection) {
    this._mongoCollection = mongoCollection;
  }

  // Does a find with "smart indexing".
  // Currently this just means, if it needs a geoindex and there is
  // none, then build the geoindex.
  // This could be improved a lot but it's not clear if that's a good
  // idea. Or even if this behavior is a good idea.
  find(query, { skip, limit, sort, keys, maxTimeMS, readPreference } = {}) {
    // Support for Full Text Search - $text
    if(keys && keys.$score) {
      delete keys.$score;
      keys.score = {$meta: 'textScore'};
    }
    return this._rawFind(query, { skip, limit, sort, keys, maxTimeMS, readPreference })
      .catch(error => {
        // Check for "no geoindex" error
        if (error.code != 17007 && !error.message.match(/unable to find index for .geoNear/)) {
          throw error;
        }
        // Figure out what key needs an index
        const key = error.message.match(/field=([A-Za-z_0-9]+) /)[1];
        if (!key) {
          throw error;
        }

        var index = {};
        index[key] = '2d';
        return this._mongoCollection.createIndex(index)
          // Retry, but just once.
          .then(() => this._rawFind(query, { skip, limit, sort, keys, maxTimeMS, readPreference }));
      });
  }

  _rawFind(query, { skip, limit, sort, keys, maxTimeMS, readPreference } = {}) {
    let findOperation = this._mongoCollection
      .find(query, { skip, limit, sort, readPreference })

    if (keys) {
      findOperation = findOperation.project(keys);
    }

    if (maxTimeMS) {
      findOperation = findOperation.maxTimeMS(maxTimeMS);
    }

    const hrstart = process.hrtime();
    const findOperationToArray = findOperation.toArray();
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return findOperationToArray.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".rawFind query: " + JSON.stringify(query) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return findOperationToArray;
    }
  }

  count(query, { skip, limit, sort, maxTimeMS, readPreference } = {}) {
    const hrstart = process.hrtime();
    const countOperation = this._mongoCollection.count(query, { skip, limit, sort, maxTimeMS, readPreference });
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return countOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".count query: " + JSON.stringify(query) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return countOperation;
    }
  }

  distinct(field, query) {
    return this._mongoCollection.distinct(field, query);
  }

  aggregate(pipeline, { maxTimeMS, readPreference } = {}) {
    return this._mongoCollection.aggregate(pipeline, { maxTimeMS, readPreference }).toArray();
  }

  insertOne(object) {
    const hrstart = process.hrtime();
    const insertOneOperation = this._mongoCollection.insertOne(object);
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return insertOneOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".insertOne object: " + JSON.stringify(object) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return insertOneOperation;
    }
  }

  // Atomically updates data in the database for a single (first) object that matched the query
  // If there is nothing that matches the query - does insert
  // Postgres Note: `INSERT ... ON CONFLICT UPDATE` that is available since 9.5.
  upsertOne(query, update) {
    const hrstart = process.hrtime();
    const upsertOneOperation = this._mongoCollection.update(query, update, { upsert: true });
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return upsertOneOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".upsertOne query: " + JSON.stringify(query) + " update " + JSON.stringify(update) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return upsertOneOperation;
    }
  }

  updateOne(query, update) {
    const hrstart = process.hrtime();
    const updateOneOperation = this._mongoCollection.updateOne(query, update);
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return updateOneOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".updateOne query: " + JSON.stringify(query) + " update " + JSON.stringify(update) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return updateOneOperation;
    }
  }

  updateMany(query, update) {
    const hrstart = process.hrtime();
    const updateManyOperation = this._mongoCollection.updateMany(query, update);
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return updateManyOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".updateMany query: " + JSON.stringify(query) + " update " + JSON.stringify(update) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return updateManyOperation;
    }
  }

  deleteMany(query) {
    const hrstart = process.hrtime();
    const deleteManyOperation = this._mongoCollection.deleteMany(query);
    if (Logger && Logger.PARSE_QUERIES && Logger.logEnabled(Logger.PARSE_QUERIES)) {
      return deleteManyOperation.then((results) => {
        const hrend = process.hrtime(hrstart);
        const ms = hrend[0] * 1000 + hrend[1] / 1000000;
        Logger.log("PARSE_QUERIES", this._mongoCollection.s.name + ".deleteMany query: " + JSON.stringify(query) + " took " + ms + "ms");
        return results;
      });
    }
    else {
      return deleteManyOperation;
    }
  }

  _ensureSparseUniqueIndexInBackground(indexRequest) {
    return new Promise((resolve, reject) => {
      this._mongoCollection.ensureIndex(indexRequest, { unique: true, background: true, sparse: true }, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  drop() {
    return this._mongoCollection.drop();
  }
}
