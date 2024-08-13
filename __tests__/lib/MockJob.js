const EventEmitter = require ('events')

const {Tracker}  = require ('events-to-winston')
const {Writable} = require ('stream')
const winston = require ('winston')
const logger = winston.createLogger({
	transports: [
//	  new winston.transports.Console (),
	  new winston.transports.Stream ({stream: new Writable ({write(){}})})
	]
})

module.exports = class extends EventEmitter {

	constructor () {

		super ()

		this.uuid = '00000000-0000-0000-0000-000000000000'

		this.logger = logger

		this [Tracker.LOGGING_ID] = 'DUMMY'

	}

}