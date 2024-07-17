const CH_OPEN = '{'.charCodeAt (0)
const PROP_JSON = Symbol ('json')
const KEYS = [
	'processId', 
	'channel', 
	'payload'
]

class DbNotificationPg {

	constructor (o) {

		for (const k of KEYS) if (k in o) this [k] = o [k]

	}

	get isJSON () {

		return this.json !== undefined

	}

	get json () {

		if (PROP_JSON in this) return this [PROP_JSON]

		const {payload} = this; if (!payload || payload.length < 2 || payload.charCodeAt (0) !== CH_OPEN) this [PROP_JSON] = undefined

		try {

			return this [PROP_JSON] = JSON.parse (payload)
			
		} 
		catch (error) {

			return this [PROP_JSON] = undefined

		}

	}

}

module.exports = DbNotificationPg