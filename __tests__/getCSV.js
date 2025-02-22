const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg, DbLangPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

afterAll(async () => {

	await pool.pool.end ()

})

test ('isCopyStatement', () => {

	const lang = new DbLangPg ()

	expect (lang.isCopyStatement (' COPY users TO STDOUT')).toBe (true)
	expect (lang.isCopyStatement (' Copy users TO STDOUT')).toBe (true)
	expect (lang.isCopyStatement (' copy users TO STDOUT')).toBe (true)
	expect (lang.isCopyStatement (' SELECT * FROM users')).toBe (false)

})


test ('drop create insert select', async () => {

	const dst = 'my_table'

	try {

		var db = await pool.setResource (job, 'db')

		for (const sql of [

			`DROP TABLE IF EXISTS ${dst}`,

			`CREATE TABLE ${dst} (
				id INT, 
				label TEXT, 
				dt DATE,
				b BOOL DEFAULT FALSE
			)`,

		]) await db.do (sql)
		
		await db.do (`INSERT INTO ${dst} (id, label, dt) VALUES (?, ?, ?)`, [1, 'o,"\r\n"ne', '2000-01-01'])
		await db.do (`INSERT INTO ${dst} (id, label, b) VALUES (?, ?, ?)`, [2, 't,wo', true])

		const s = await db.getStream (`COPY ${dst} TO STDOUT (FORMAT CSV)`)

		const a = []; for await (const r of s) a.push (r.toString ())

		expect (a).toStrictEqual ([
			'1,"o,""\r\n""ne",2000-01-01,f\n', 
			'2,"t,wo",,t\n'
		])

	}
	finally {

		await db.release ()

	}
	
})