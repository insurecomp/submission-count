const fastify = require("../fastify");
const dumpHandlerService = require("../services/dumpHandler.service");

class RunnerRoute {
  constructor() {}

  registerRoutes() {
    fastify.get(`/dev/api/dataDump`, this.processDataDump.bind(this));
  }

  async processDataDump() {
    return dumpHandlerService.processDataDump();
  }
}

module.exports = new RunnerRoute();
