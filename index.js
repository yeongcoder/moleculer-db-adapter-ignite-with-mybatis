/*eslint-disable */
const { MoleculerServerError } = require("moleculer").Errors;
const IgniteClient = require("apache-ignite-client");
const mybatisMapper = require("mybatis-mapper");

const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const CacheConfiguration = IgniteClient.CacheConfiguration;

class IgniteAdapter {
  /**
   * Creates an instance of IgniteAdapter.
   * @param {Object} opts
   *
   * @memberof IgniteAdapter
   */
  constructor(opts) {
    this.opts = opts;
    this.mapper = mybatisMapper;
  }

  /**
   * Initialize adapter
   *
   * @param {ServiceBroker} broker
   * @param {Service} service
   *
   * @memberof IgniteAdapter
   */
  init(broker, service) {
    this.broker = broker;
    this.service = service;
  }

  /**
   * Connect to database
   *
   * @returns {Promise}
   *
   * @memberof IgniteAdapter
   */
  connect() {
    if (!this.service.schema.settings.mapperDir) {
      throw new MoleculerServerError(
        "Missing `mapperDir` definition in schema.settings of service!"
      );
    }
    this.client = new IgniteClient((state, reason) => {
      this.service.logger.info("Ignite StateChanged", state, reason);
    });
    this.configuration = new IgniteClientConfiguration(this.opts.host)
      .setUserName(this.opts.user) // https://ignite.apache.org/docs/latest/security/authentication
      .setPassword(this.opts.password) // https://www.bswen.com/2018/10/java-How-to-solve-Apache-ignite-IgniteException-Can-not-perform-the-operation-because-the-cluster-is-inactive.html
      .setConnectionOptions(this.opts.useTLS, this.opts.connectionOptions); // https://ignite.apache.org/docs/latest/thin-clients/nodejs-thin-client#partition-awareness
    this.mapper.createMapper(this.service.schema.settings.mapperDir);
    return this.client
      .connect(this.configuration)
      .then(() =>
        this.client.getOrCreateCache(
          this.opts.cache,
          new CacheConfiguration.setSqlSchema(this.opts.schema)
        )
      )
      .then((cache) => {
        this.db = cache;
        this.db.sendQuery = this.sendQuery.bind(this);
        this.service.logger.info("Ignite adapter has connected successfully.");
      });
  }

  /**
   * Disconnect from database
   *
   * @returns {void}
   *
   * @memberof IgniteAdapter
   */
  disconnect() {
    if (this.client) {
      this.client.disconnect();
    }
    return Promise.resolve();
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} namespace
   * @param {string} id
   * @param {object?} params
   * @returns {Promise}
   *
   * @memberof IgniteAdapter
   */
  async sendQuery(namespace, id, params) {
    const sql = this.mapper.getStatement(namespace, id, params);
    //this.service.logger.info(sql);
    try {
      let results = [];
      const sqlFieldsQuery = new SqlFieldsQuery(sql).setIncludeFieldNames(true);
      const cursor = await this.db.query(sqlFieldsQuery);

      do {
        let value = await cursor.getValue();
        let row = {};
        for (let i = 0; i < cursor._fieldNames.length; i++) {
          row[cursor._fieldNames[i]] = value[i];
        }
        results.push(row);
      } while (cursor.hasMore());

      return results;
    } catch (err) {
      this.service.logger.error("err: ", err);
      return err;
    }
  }
}

module.exports = IgniteAdapter;
