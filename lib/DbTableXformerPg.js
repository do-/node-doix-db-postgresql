const {Transform} = require ('stream')
const {DbTable} = require ('doix-db')

module.exports = class extends Transform {

	constructor () {

		super ({readableObjectMode: true, writableObjectMode: true})
		
	}

	_transform (_src, encoding, callback) {
		
		this.push (new DbTable (_src))

		callback ()

	}

}