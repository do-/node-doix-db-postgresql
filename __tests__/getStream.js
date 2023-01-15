const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')

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

test ('one record', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const s = await db.getStream ('SELECT 1 id', [])

		const a = []; for await (const r of s) a.push (r)

		expect (a).toStrictEqual ([{id: 1}])

	}
	finally {

		await db.release ()

	}
	
})

test ('arrays', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const s = await db.getStream ('SELECT * FROM generate_series (?::int, ?) id', [1, 10], {rowMode: 'array'})

		const a = []; for await (const r of s) a.push (r)

		expect (a).toStrictEqual ([[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]])

	}
	finally {

		await db.release ()

	}
	
})