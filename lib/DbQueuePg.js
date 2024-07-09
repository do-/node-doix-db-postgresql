const {Queue} = require ('doix')

class DbQueuePg extends Queue {

	constructor (app, o) {

        const {view} = o

        if (!('order' in o)) throw Error ('DbQueuePg: order not set for ' + view.name)

		super (app, o)

        this.order = o.order
        this.view  = view

	}

    async peek (job) {

        const {view} = this

        return job [view.model.db.name].peek (view)

    }
	
}

module.exports = DbQueuePg