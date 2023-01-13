const EventEmitter = require ('events')
const {DbClientPg, DbPoolPg} = require ('..'), {normalizeSQL} = DbClientPg

const job = new EventEmitter ()
job.uuid = '00000000-0000-0000-0000-000000000000'
job.logger = {log: ({message, level}) => {if (false) console.log (level + ' ' + message)}}

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

afterAll(async () => {

	await pool.pool.end ()

})

test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const s = await db.getStream ('...')	

		await expect (			
			(async () => {
				const a = []; for await (const r of s) a.push (r)				
			})()			
		).rejects.toThrow ()

	}
	finally {

		await db.release ()

	}
	
})

test ('getStream 1', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const s = await db.getStream ('SELECT 1 id')

		const a = []; for await (const r of s) a.push (r)

		expect (a).toStrictEqual ([{id: 1}])

	}
	finally {

		await db.release ()

	}
	
})