const {Transform} = require ('stream')

module.exports = class extends Transform {

	constructor (model) {

		super ({readableObjectMode: true, writableObjectMode: true})
		
		this.schemata = new Map ()

		for (const s of model.schemata.values ()) this.schemata.set (s.schemaName, s)

	}

	_transform (_src, encoding, callback) {

		try {

			callback (null, this.schemata.get (_src.table_schema).create (_src))
			
		}
		catch (x) {

			callback (x)

		}
	
	}

}