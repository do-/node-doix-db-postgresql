const Path = require ('path')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

	var db = await pool.toSet (job, 'db')

	const nop = _ => null

	db.do ('SELECT pg_sleep (1000000)').then (nop, nop)

	await db.release ()

})