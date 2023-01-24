const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('getCanonicalTypeName', () => {

	expect (() => lang.getCanonicalTypeName ('')).toThrow ()

	expect (lang.getCanonicalTypeName ('INT4')).toBe ('INT4')
	expect (lang.getCanonicalTypeName ('int')).toBe ('INT4')
	expect (lang.getCanonicalTypeName ('numeric')).toBe ('NUMERIC')
	expect (lang.getCanonicalTypeName ('Decimal')).toBe ('NUMERIC')
	expect (lang.getCanonicalTypeName ('string')).toBe ('TEXT')

})