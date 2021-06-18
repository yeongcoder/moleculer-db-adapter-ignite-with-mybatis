/*eslint-disable */
const _ = require("lodash");
const { ServiceSchemaError } = require("moleculer").Errors;
const IgniteClient = require("apache-ignite-client");
const mybatisMapper = require("mybatis-mapper");

const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const CacheConfiguration = IgniteClient.CacheConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const QueryEntity = IgniteClient.QueryEntity;
const QueryField = IgniteClient.QueryField;

class IgniteAdapter {
  constructor(opts) {
    this.opts = opts;
    this.mapper = _.cloneDeep(mybatisMapper);
  }
  /**
   * Initialize adapter
   *
   * @param {ServiceBroker} broker
   * @param {Service} service
   *
   * @memberof MariaAdapter
   */
  init(broker, service) {
    this.broker = broker;
    this.service = service;
  }

  connect() {
    this.client = new IgniteClient((state, reason) => {
      this.service.logger.info("Ignite StateChanged", state, reason);
    });
    this.configuration = new IgniteClientConfiguration(this.opts.host)
      .setUserName(this.opts.user) // https://ignite.apache.org/docs/latest/security/authentication
      .setPassword(this.opts.pwd) // https://www.bswen.com/2018/10/java-How-to-solve-Apache-ignite-IgniteException-Can-not-perform-the-operation-because-the-cluster-is-inactive.html
      .setConnectionOptions(
        false,
        {
          timeout: Number(this.opts.timeout),
        },
        true
      ); // https://ignite.apache.org/docs/latest/thin-clients/nodejs-thin-client#partition-awareness
    this.mapper.createMapper([this.service.schema.settings.mapperDir]);
    return this.client
      .connect(this.configuration)
      .then(() => this.client.getCache(this.opts.cache))
      .then((cache) => {
        this.db = cache;
        this.db.queryExecute = this.queryExecute.bind(this);
        this.service.logger.info("Ignite adapter has connected successfully.");
      });
  }

  disconnect() {
    return this.client.disconnect();
  }

  async queryExecute(id, params) {
    const sql = this.mapper.getStatement("ignite", id, params);
    this.service.logger.info(sql);
    try {
      let results = [];
      const sqlFieldsQuery = new SqlFieldsQuery(sql).setIncludeFieldNames(true);
      const cursor = await this.db.query(sqlFieldsQuery);
      this.service.logger.info("sqlFieldsQuery: ", sqlFieldsQuery);
      this.service.logger.info("cursor: ", cursor);

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
