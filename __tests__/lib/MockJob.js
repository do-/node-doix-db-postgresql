const EventEmitter = require ('events')

module.exports = class extends EventEmitter {

	constructor () {

		super ()

		this.uuid = '00000000-0000-0000-0000-000000000000'
		
		this.logger = {
		
			log: ({message, level}) => {
			
				//if (false) 
				console.log (level + ' ' + message)
				
			}
		
		}

	}

}