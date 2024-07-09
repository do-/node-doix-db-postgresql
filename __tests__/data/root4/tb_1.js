module.exports = {

    comment: 'Table 2',

    columns: {
        id       : 'int    // PK',
    },
    
    pk: 'id',

    triggers: [

    	{
			phase  : 'AFTER INSERT',
			action : 'FOR EACH ROW',
			sql    : `
				BEGIN
					NOTIFY hotline, 'q_1';
					RETURN NEW;
				END;
			`,
    	},

    ],


}