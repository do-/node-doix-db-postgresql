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

		const dbName = 'doix_test_db_3'

		const model = new DbModel ({dir, db: pool})

		var db = await pool.toSet (job, 'db')

		await db.do (`DROP SCHEMA IF EXISTS ${dbName} CASCADE`)
		await db.do (`CREATE SCHEMA ${dbName}`)
		await db.do (`SET SCHEMA '${dbName}'`)

//		expect ([...db.lang.genDDL ()]).toHaveLength (0)

		model.loadModules ()
		
		const plan = db.createMigrationPlan ()

		await plan.loadStructure ()
		plan.inspectStructure ()

		for (const [sql] of plan.genDDL ()) await db.do (sql)

		{
		
			await db.do ('INSERT INTO tb_1 (id) VALUES (?)', [1])
			
			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([{id: 1, label: 'on', amount: '0.00'}])
			
			}

			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label', [1, 'one'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([{id: 1, label: 'one', amount: '0.00'}])
			
			}

		}
		
		{

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			expect ([...plan.toDo.keys ()]).toStrictEqual (['recreate'])

			expect (plan.asIs.get ('tb_1').toDo.size).toStrictEqual (0)

		}

		{
		
			await db.do (`ALTER TABLE tb_1 DROP COLUMN amount CASCADE`)

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([{id: 1, label: 'one'}])
			
			}

			for (const [sql] of plan.genDDL ()) await db.do (sql)

			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([{id: 1, label: 'one', amount: '0.00'}])
			
			}

		}
		
		{
		
			await db.do (`ALTER TABLE tb_1 ALTER amount DROP NOT NULL, ALTER amount SET DEFAULT 1, ALTER amount TYPE DECIMAL(5,1), ALTER label SET NOT NULL, ALTER id SET DEFAULT 0`)

			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?)', [2, 'two'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([
					{id: 1, label: 'one', amount: '0.0'},
					{id: 2, label: 'two', amount: '1.0'}
				])
			
			}

			await db.do ('UPDATE tb_1 SET amount = NULL WHERE id = ?', [2])

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			for (const [sql, params] of plan.genDDL ()) await db.do (sql, params)

			{

				const a = await db.getArray ('SELECT * FROM tb_1')

				expect (a).toStrictEqual ([
					{id: 1, label: 'one', amount: '0.00'},
					{id: 2, label: 'two', amount: '0.00'}
				])
			
			}

		}

		{

			const a = await db.getArray ('SELECT * FROM vw_1')

			expect (a).toStrictEqual ([{id: 1}])

		}

		{

			const o = await db.getObject ('vw_1', [1])

			expect (o).toStrictEqual ({id: 1})
		}
		
		{

			const o = await db.getObject ('vw_1', 1)

			expect (o).toStrictEqual ({id: 1})

		}

		{

			const o = await db.getObject ('vw_1', [2])

			expect (o).toStrictEqual ({})

		}

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
			expect (l [Symbol.for ('query')]).toBe (qc)
		
		}

		await db.do (`DROP SCHEMA ${dbName} CASCADE`)

	}
	finally {

		await db.release ()

	}

})