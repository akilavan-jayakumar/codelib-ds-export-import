const axios = require('axios').default;
const express = require('express');

let doNotMakeRequest = false;

(async () => {
	try {
		const offset = 200;
		for (let start = 0; start <= 300000 - offset; start += offset) {
			await axios
				.get(
					'https://dsexportimportcodelib-773793963.development.catalystserverless.com/server/data-populate/execute',
					{
						params: {
							offset: start + 1
						}
					}
				)
				.then((response) => {
					console.log(start + 1, start + offset, JSON.stringify(response.data));
				})
				.catch((error) => {
					console.log(error);
				});
		}

		doNotMakeRequest = true;
	} catch (err) {
		console.log(err);
	}
})();

(async () => {
	try {
		let count = 1;
		while (true) {
			console.log('Request started count ::: ', count);
			await axios
				.get(
					'https://appsail-10079964142.development.catalystappsail.com/health',
					{
						timeout: 5000
					}
				)
				.then(() => {
					console.log('Request ended count ::: ', count);
				})
				.catch((error) => {
					console.log(error);
				});

			await new Promise((resolove) => setTimeout(resolove, 1000));

			count++;

			if (doNotMakeRequest) {
				break;
			}
		}
	} catch (err) {
		console.log(err);
	}
})();

const app = express();

app.get('*', (req, res) => {
	res.send('ok');
});

app.listen(process.env.X_ZOHO_CATALYST_LISTEN_PORT || 9000, () => {
	console.log('Server Started');
});
