module.exports = {

    label: 'Table 1',

    columns: {
        id       : 'int    // PK',
        label    : 'text?=on   // Human readable',
        amount   : 'decimal(10,2)=0 // Some money',
        cnt      : 'int=0 // Mutations counter',
    },

    pk: 'id',

    triggers: [

    	{

//			options: 'CONSTRAINT',

			phase  : 'BEFORE INSERT OR UPDATE',

			action : 'FOR EACH ROW /*WHEN (TRUE)*/',

			sql    : `

				BEGIN

					NEW.cnt = NEW.cnt + 1;

					RETURN NEW;

				END;

			`,

    	},

    ],

}