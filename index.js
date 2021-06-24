/*eslint-disable */
const _ = require("lodash");
const { MoleculerServerError } = require("moleculer").Errors;
const IgniteClient = require("apache-ignite-client");
const mybatisMapper = require("mybatis-mapper");

const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const CacheConfiguration = IgniteClient.CacheConfiguration;

class IgniteAdapter {
  /**
   * Creates an instance of IgniteAdapter.
   * @param {string} url
   * @param {object} opts
   *
   * @memberof IgniteAdapter
   */
  constructor(url, opts) {
    this.url = url;
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
    const host = this._getHostFromUrl(this.url);
    const user = this._getUserFromUrl(this.url);
    const password = this._getPasswordFromUrl(this.url);
    const schema = this._getSchemaFromUrl(this.url);
    const useTls = this.opts.useTls;
    const connectionOption = this._getConnectOptions(this.opts);
    if (!this.service.schema.settings.mapperDir) {
      throw new MoleculerServerError(
        "Missing `mapperDir` definition in schema.settings of service!"
      );
    }
    this.client = new IgniteClient((state, reason) => {
      this.service.logger.info("Ignite StateChanged", state, reason);
    });
    const igniteConfiguration = new IgniteClientConfiguration(host)
      .setUserName(user)
      .setPassword(password)
      .setConnectionOptions(useTls, connectionOption);
    this.mapper.createMapper(this.service.schema.settings.mapperDir);
    return this.client
      .connect(igniteConfiguration)
      .then(() =>
        this.client.getOrCreateCache(
          this.opts.cache,
          new CacheConfiguration().setSqlSchema(schema)
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
    this.service.logger.info(sql);
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

  /**
   * Send SQL Query to Database
   *
   * @returns {object}
   *
   * @memberof MariaDbAdapter
   */
  _getConnectOptions(option) {
    const optionCopy = _.cloneDeep(option);
    delete optionCopy.useTls;
    return optionCopy;
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} url
   * @returns {string}
   *
   * @memberof MariaDbAdapter
   */
  _getHostFromUrl(url) {
    return url.match(/@(.*:\d*)/g)[0].replace("@", "");
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} url
   * @returns {string}
   *
   * @memberof MariaDbAdapter
   */
  _getSchemaFromUrl(url) {
    return url.match(/\/\w*$/g)[0].replace("/", "");
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} url
   * @returns {string}
   *
   * @memberof MariaDbAdapter
   */
  _getUserFromUrl(url) {
    return url.match(/\/\w*(?=:)/g)[0].replace("/", "");
  }

  /**
   * Send SQL Query to Database
   *
   * @param {string} url
   * @returns {string}
   *
   * @memberof MariaDbAdapter
   */
  _getPasswordFromUrl(url) {
    return url.match(/\:\w*(?=@)/g)[0].replace(":", "");
  }
}

module.exports = IgniteAdapter;
