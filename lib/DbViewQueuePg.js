const {DbView} = require ('doix-db')
const DbQueuePg = require ('./DbQueuePg.js')

class DbViewQueuePg extends DbView {

	setLang (lang) {

		super.setLang (lang)
		
		this.queue = new DbQueuePg (this.model.db.app, {...this.queue, view: this})

	}

}

module.exports = DbViewQueuePg