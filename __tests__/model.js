const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const r = () => ['root1'].map (i => Path.join (__dirname, 'data', i))

const dir = {
	root: r (),
	live: false,
}

afterAll(async () => {

	await pool.pool.end ()

})

test ('model', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const model = new DbModel ({dir, db: pool})

		expect ([...db.lang.genDDL (model)]).toHaveLength (0)

		model.loadModules ()

		for (const [sql] of db.lang.genDDL (model)) await db.do (sql)
		
		const a = await db.getArray ('SELECT * FROM vw_1')

		expect (a).toStrictEqual ([{id: 1}])

		const q = model.createQuery ([
			['vw_1', {
				columns: ['id'],
				filters: [['id', '=', 1]],
			}],
		])		

		const id = await db.getScalar (q)

		expect (id).toBe (1)

		const is = await db.getStream (q)
		
		for await (const r of is) expect (r).toStrictEqual ({id: 1})
		
		{

			const qc = model.createQuery ([['vw_1']], {
				order: ['id'],
				limit: 1,
				offset: 0,
			})		

			const l = await db.getArray (qc)

			expect (l).toStrictEqual ([{id: 1}])
			expect (l [Symbol.for ('count')]).toBe (1)
		
		}

	}
	finally {

		await db.release ()

	}

})