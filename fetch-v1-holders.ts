import {writeFileSync} from 'fs';
import {createActor} from './declarations/main';

let options = {
  agentOptions: {
    host: 'https://icp-api.io',
  }
};

let actor = createActor('2v5zm-uaaaa-aaaae-qaewa-cai', options);

(async () => {
  let punkHolders = (await actor.getRegistry()).map((x) => x[1]);
  console.log('punk holders', punkHolders.length);

  writeFileSync('v1-holders.txt', '"' + punkHolders.join('";\n"') + '";');
})();