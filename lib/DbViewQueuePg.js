const {DbView} = require ('doix-db')

class DbViewQueuePg extends DbView {

	constructor (o) {

		super (o)		

	}

	setRq (job, record) {

		job.rq = record === null ? {} : {...this.queue.rq, ...record}

	}

}

module.exports = DbViewQueuePg