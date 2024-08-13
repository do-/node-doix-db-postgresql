const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const { execPath } = require('process')
const {DbPoolPg} = require ('..')
const {Writable} = require ('stream')
const winston = require ('winston')
const normalizeSpaceLogFormat = require ('string-normalize-space').logform

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const schemaName = 'notice_test', procName = schemaName + '.test'

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

	let s = ''

	const stream = new Writable ({
		write (r, _, cb) {
			s += r.toString ()
			cb ()
		}

	})

	const tr = new winston.transports.Stream ({
		stream,
		format: winston.format.combine (
			normalizeSpaceLogFormat ()
			, winston.format.printf ((i => `${i.event} ${i.message} ${i.details ? JSON.stringify (i.details) : ''}`.trim ()))
		)
	})
	
	pool.logger = job.logger

	pool.logger.add (tr)

	try {

		var db = await pool.toSet (job, 'db')

		await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)
		await db.do (`CREATE SCHEMA ${schemaName}`)
		await db.do (`CREATE OR REPLACE PROCEDURE ${procName} () LANGUAGE PLPGSQL AS $$ 
			BEGIN
				RAISE NOTICE 'test notice';
			END; 
		$$`)

		await db.do (`CALL ${procName} ()`)
		await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)

		const a = s.trim ().split ('\n').map (s => s.trim ()).filter (s => /^notice /.test (s))

		expect (a).toHaveLength (3)
		expect (a [1]).toMatch (/^notice test notice/)

	}	
	finally {

		pool.logger.remove (tr)

		await db.release ()

	}

})