const { AsyncTask, CronJob } = require("toad-scheduler");
const fastify = require("../fastify");
const DataDumpService = require("./dumpHandler");

class RunnerService {
  async runDataDumpJob() {
    const task = new AsyncTask("Data Dump", async () => {
      await DataDumpService.processDataDump();
    });

    /** Runs at 6:00 PM on Fridays only */
    const job = new CronJob("0 17 * * 5", task);
    fastify.scheduler.addCronJob(job);
  }

  async run() {
    await this.runDataDumpJob();
  }
}

module.exports = new RunnerService();
