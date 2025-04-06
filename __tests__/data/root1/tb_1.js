module.exports = {

    comment: 'Table 1',

    columns: {
        id       : 'int    // PK',
        label    : "text?='on'   // Human readable",
        amount   : 'decimal(10,2)=0 // Some money',
        cnt      : 'int=0 // Mutations counter',
    },

    pk: 'id',

    data: [
        {id: 0, label: 'zero'},
    ],
    
    keys: {

        anomaly: null,
        
        amount   : 'amount',
        
        label   : {
        	parts:   ['label'],
        	options: ['UNIQUE', 'WHERE cnt > 0'],
            message: ({detail}) => /\=\((.*?)\)/.exec (detail) [1]
        },

        label0   : {
        	parts:   ['label'],
        	options: ['UNIQUE', 'WHERE cnt = 0'],
        },

    },

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

    	{

            phase  : 'AFTER INSERT OR UPDATE',

            action : 'FOR EACH ROW /*WHEN (TRUE)*/',

            sql    : null,
            
        },
    ],

}