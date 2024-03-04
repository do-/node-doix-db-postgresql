const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()

const a = []; job.logger = {log: s => a.push (s)}

const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const schemaName = 'notice_test', procName = schemaName + '.test'

pool.logger = job.logger

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

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

		expect (a.filter (i => i.level === 'notice' && i.message.indexOf ('test notice') > -1)).toHaveLength (1)

	}
	
	finally {

		await db.release ()

	}

})