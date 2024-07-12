const {JobSource} = require ('doix')
const DbPoolPg = require ('./DbPoolPg')
const DbViewQueuePg = require ('./DbViewQueuePg')

const NOP = () => {}

class DbChannelPg extends JobSource {

	constructor (app, o) {

		if (!('name' in o)) throw Error ('DbChannelPg: name not set')

		const {name} = o

		if (typeof name !== 'string') throw Error ('DbChannelPg: name must be a string')

		if (name.length === 0) throw Error ('DbChannelPg: name must be a non empty string')

		super (app, o)

		this.name = name

		this.test = ({channel}) => channel === name

		this.models = new Set ()
		
	}

	getQueue (notification) {

		for (const model of this.models) {

			const view = model.find (notification.payload)

			if (view instanceof DbViewQueuePg) return view.queue

		}

	}

	get router () {

		return this.listener

	}

	set router (listener) {

		this.listener = listener

		const {pool} = listener; if (pool instanceof DbPoolPg)

			for (const [_, db] of this.app.pools)

				if (pool.isSameDbAs (db)) {

					const {model} = db

					this.models.add (model)

					for (const view of model.objects ())

						if (view instanceof DbViewQueuePg)

							view.queue.check ()

				}

	}

	process (notification) {

		const q = this.getQueue (notification); if (q) return q.check ()

		const job = this.createJob ()

		job.notification = notification

		job.toComplete ().then (NOP, NOP)

	}

}

module.exports = DbChannelPg