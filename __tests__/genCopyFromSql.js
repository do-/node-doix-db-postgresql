const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('genCopyFromSqlOptionValue', () => {

	expect (lang.genCopyFromSqlOptionValue ('FORMAT', 'CSV')).toBe ('CSV')
	expect (lang.genCopyFromSqlOptionValue ('FORCE_QUOTE', '*')).toBe ('*')
	expect (lang.genCopyFromSqlOptionValue ('FORCE_QUOTE', ['id', 'label'])).toBe ('("id","label")')
	expect (lang.genCopyFromSqlOptionValue ('OIDS', false)).toBe ('FALSE')
	expect (lang.genCopyFromSqlOptionValue ('NULL', '\N')).toBe ("'\N'")

})

test ('genCopyFromSqlOptions', () => {

	expect (lang.genCopyFromSqlOptions ({})).toBe ('')
	expect (lang.genCopyFromSqlOptions ({
		format: 'CSV',
		force_quote: '*',
		OIDS: false,
		NULL: '\N'
	})).toBe (" WITH (FORMAT CSV, FORCE_QUOTE *, NULL '\N')")

})

test ('genCopyFromSql', () => {

	expect (lang.genCopyFromSql ('roles', ['id', 'label'], {
		format: 'CSV',
		force_quote: '*',
		OIDS: false,
		NULL: '\N'
	})).toBe (`COPY "roles" ("id","label") FROM STDIN WITH (FORMAT CSV, FORCE_QUOTE *, NULL '\N')`)

})