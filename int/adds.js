const Dir = '/Users/shenyang/Desktop/base/';
const ACCESS_KEY = 'mdstorage';
const SECRET_KEY = 'eBxExGQJNhGosgv5FQJiVNqH';
//const Host = 'http://localhost:9000';
const Host = 'http://132.232.203.210:34991';

const glob = require('glob');
const fs = require('fs');
const path = require('path');
const util = require('util');
const exec = util.promisify(require('child_process').exec);


init().then(()=>{
  console.log('finish')
})



async function init () {
  await exec('mc config host add s1 ' + Host + ' ' + ACCESS_KEY + '  ' + ' ' +
    SECRET_KEY);

  // await exec('mc mb s1/mdpic');
  // await exec('mc mb s1/mdoc');
  // await exec('mc mb s1/mdmedia');
  // await exec('mc mb s1/mdpub');

  glob(Dir + '**/**', {nonull: false, nodir: true}, async function (er, files) {
    // files is an array of filenames.
    // If the `nonull` option is set, and nothing
    // was found, then files is ["**/*.js"]
    // er is an error object or null.
    // console.log(files);
    await Promise.all(files.map(async file => {
      var key = file.replace(Dir, '');
      try {
        var {stdout, stderr} = await exec('mc cp ' + file + ' s1/' +
          key,
          {timeout: 200000, maxBuffer: 1000 * 1024});
        console.log('mc cp' + file + ' s1/' + key);
        // fs.unlinkSync(path);
      } catch (e) {
        console.log('err', e);
        // console.log('exec use time  ', new Date() - a);
      }
    }));
  });
}
