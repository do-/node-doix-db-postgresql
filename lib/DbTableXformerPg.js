const {Transform} = require ('stream')
const {DbTable} = require ('doix-db')

module.exports = class extends Transform {

	constructor (lang) {

		super ({readableObjectMode: true, writableObjectMode: true})
		
		this.lang = lang
		
	}

	_transform (_src, encoding, callback) {
	
		const {lang} = this, table = new DbTable (_src), {columns} = table
		
		for (const name in columns) columns [name].setLang (lang)
		
		this.push (table)

		callback ()

	}

}