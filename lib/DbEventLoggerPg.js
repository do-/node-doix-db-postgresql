const {EventLogger} = require ('doix')

class DbEventLoggerPg extends EventLogger {

	constructor (client) {

		super (client)

		this.client = client

		this.logger = client.logger

	}

	get prefix () {
	
		let {client} = this, {job} = client

		let p = job.uuid + '/' + client.uuid

		while (job = job.parent) p = j.uuid + '/' + p

		return p

	}
	
	startMessage ({sql, params}) {

		return this.message ('> ' + sql.split (/\s+/).join (' ').trim () + ' ' + JSON.stringify (params))

	}

}

module.exports = DbEventLoggerPg