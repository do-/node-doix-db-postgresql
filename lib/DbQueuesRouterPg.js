const {Tracker} = require ('events-to-winston')
const {Router: {TEST_MESSAGE, PROCESS_MESSAGE}} = require ('protocol-agnostic-router')

class DbQueuesRouterPg {

	constructor (app, o) {

		if (!o || !o.name || !(typeof o.name === 'string')) throw Error ('DbQueuesRouterPg: name not properly set')

		this.name = o.name

		this.app = app		

		this.models = new Set ()

		if ('test' in o) this [TEST_MESSAGE] = o.test

		this.on = 'on' in o ? Object.entries (o.on).map (([k, v]) => ([k, Array.isArray (v) ? v : [v]])) : []
		
	}

	get [Tracker.LOGGING_ID] () {

		return this.name

	}

	getQueueName (notification) {

		if (notification == null) throw Error ('Empty notification')
		
		if (typeof notification !== 'object') throw Error ('Not a notification: ' + notification)
	
		const {payload} = notification
	
		if (payload == null) throw Error ('Empty notification payload')
				
		if (typeof payload !== 'string') throw Error ('Invalid notification payload: ' + payload)
			
		if (payload.length === 0) throw Error ('Empty notification payload')
	
		return payload

	}

	getQueue (notification) {

		const name = this.getQueueName (notification)

		for (const model of this.models) {

			const view = model.find (name); if (!view) continue

			const {queue} = view; if (!queue) throw Error ('Not a queue: ' + name)

			return queue

		}

		throw Error ('Queue not found: ' + name)

	}

	get router () {

		return this.listener

	}

	set router (listener) {

		const {pool} = listener = this.listener = listener

		if (pool) 
			
			for (const [_, db] of this.app.pools) 
				
				if (pool.isSameDbAs (db)) {

					const {model} = db

					this.models.add (model)

					for (const view of model.objects ())

						if ('queue' in view)

							this.registerQueue (view.queue)

				}

	}

	registerQueue (queue) {

		for (const [event, handlers] of this.on)

			for (const handler of handlers)

				queue.on (event, handler)

		queue.check ()

	}

	[PROCESS_MESSAGE] (notification) {

		this.getQueue (notification).check ()

	}

}

module.exports = DbQueuesRouterPg