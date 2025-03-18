const { AsyncTask, CronJob } = require("toad-scheduler");
const fastify = require("../fastify");
const DataDumpService = require("./dumpHandler");

class RunnerService {
  async runDataDumpJob() {
    const task = new AsyncTask("Data Dump", async () => {
      await DataDumpService.processDataDump();
    });

    const job = new CronJob(
      {
        /**  6:00 PM IST daily */
        cronExpression: "30 12 * * 5",
      },
      task
    );

    fastify.scheduler.addCronJob(job);
  }

  async run() {
    await this.runDataDumpJob();
  }
}

module.exports = new RunnerService();
