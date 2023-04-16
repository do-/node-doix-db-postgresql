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

		const s = await db.getScalar ('...')	

	}
	catch (x) {

		expect (x).toBeInstanceOf (Error)

	}
	finally {

		await db.release ()

	}
	
})

test ('sequence', async () => {
	
	try {
	
		pool.setProxy (job, 'db')

		const o = await job.db.getScalar ('SELECT * FROM generate_series (?::int, ?) id', [1, 10])

		expect (o).toBe (1)

	}
	finally {

		await job.db.release ()

	}
	
})

test ('1-to-1', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const o = await db.getScalar ('SELECT 1 id', [])

		expect (o).toBe (1)

	}
	finally {

		await db.release ()

	}
	
})

test ('default', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const o = await db.getScalar ('SELECT 1 id WHERE false', [])

		expect (o).toBeUndefined ()

	}
	finally {

		await db.release ()

	}
	
})

test ('custom default', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const DEF = -1

		const o = await db.getScalar ('SELECT 1 id WHERE false', [], {notFound: DEF})

		expect (o).toBe (DEF)

	}
	finally {

		await db.release ()

	}
	
})

test ('custom error', async () => {
	
	const DEF = new Error ('Not Found')

	try {
	
		var db = await pool.toSet (job, 'db')
		
		const o = await db.getScalar ('SELECT 1 id WHERE false', [], {notFound: DEF})

	}
	catch (x) {

		expect (x).toBe (DEF)

	}
	finally {

		await db.release ()

	}
	
})
