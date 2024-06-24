const catalyst = require('zcatalyst-sdk-node');

const MAX_RECORDS = 15000;
const MAX_RECORDS_PER_OPERATION = 200;

module.exports = async (context, basicIO) => {
	try {
		const start = parseInt(basicIO.getArgument('start'));
		const end = start + MAX_RECORDS;

		const catalystApp = catalyst.initialize(context);

		const circuit = catalystApp.circuit();
		const zcql = catalystApp.zcql();

		const aTable = catalystApp.datastore().table('A');
		const bTable = catalystApp.datastore().table('B');
		const cTable = catalystApp.datastore().table('C');
		const dTable = catalystApp.datastore().table('D');

		const totalRecords = await zcql
			.executeZCQLQuery('SELECT COUNT(ROWID) FROM A')
			.then((records) => parseInt(records[0]['A']['ROWID']));

		if (totalRecords >= 800000) {
			basicIO.write('end');
		} else {
			for (
				let offset = start;
				offset < end;
				offset += MAX_RECORDS_PER_OPERATION
			) {
				const records = Array.from(
					{ length: MAX_RECORDS_PER_OPERATION },
					(_, index) => ({
						VALUE: offset + index + 1
					})
				);

				const aRecords = await aTable.insertRows(records);

				await bTable.insertRows(
					records.map((record, index) => ({
						...record,
						AID: aRecords[index].ROWID
					}))
				);

				const cRecords = await cTable.insertRows(records);

				await cTable.updateRows(
					cRecords.map(({ ROWID }) => ({
						ROWID,
						CID: ROWID
					}))
				);

				await dTable.insertRows(records);
			}

			await circuit.execute('12130000006059759', Date.now() + '-' + end, {
				start: end
			});
		}

		basicIO.write('ok');
	} catch (err) {
		console.log(err);
		basicIO.write('error');
	}

	context.close();
};
