import {writeFileSync} from 'fs';
import {createActor} from './declarations/main';

let options = {
  agentOptions: {
    host: 'https://icp-api.io',
  }
};

let actor = createActor('hcsnx-2iaaa-aaaam-ac4bq-cai', options);

(async () => {
  let holders = (await actor.getRegistry()).map((x) => x[1]).filter((x) => x !== '0000');
  let purshased = holders.length;
  let remaining = 2200 - purshased;

  let addrByCount = holders.reduce((acc, x) => {
    acc[x] = (acc[x] || 0) + 1;
    return acc;
  }, {});

  let total = 0;
  let totalAirdrop = 0;
  let airdropByAddr = {};

  for (let [addr, count] of Object.entries<number>(addrByCount)) {
    console.log(addr, count);
    let pie = count / purshased;
    let airdrop = Math.round(remaining * pie);
    total += pie;
    totalAirdrop += airdrop;

    airdropByAddr[addr] = airdrop;

    console.log('pie', count, pie);
    console.log('airdrop', airdrop);
  }

  console.log('-'.repeat(80));
  console.log('purshased', purshased);
  console.log('remaining', remaining);
  console.log('total pie', total);
  console.log('airdrop total', totalAirdrop);

  // writeFileSync('v2-holders.txt', '"' + holders.join('";\n"') + '";');
  writeFileSync('v2-holders.txt', JSON.stringify(addrByCount, null, 2));
  writeFileSync('v2-airdrop.txt', JSON.stringify(airdropByAddr, null, 2));
})();