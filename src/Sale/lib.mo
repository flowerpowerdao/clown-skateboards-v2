import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import List "mo:base/List";
import Nat "mo:base/Nat";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Random "mo:base/Random";
import Result "mo:base/Result";
import Time "mo:base/Time";
import TrieMap "mo:base/TrieMap";
import Buffer "mo:base/Buffer";
import Text "mo:base/Text";
import Debug "mo:base/Debug";

import Root "mo:cap/Root";
import Fuzz "mo:fuzz";
import Account "mo:account";
import ICRC1 "mo:icrc1-types";

import Types "types";
import RootTypes "../types";
import Utils "../utils";

module {
  public class Factory(config : RootTypes.Config, deps : Types.Dependencies) {
    let openEdition = switch (config.sale) {
      case (#supply(_)) false;
      case (#duration(_)) true;
    };

    /*********
    * STATE *
    *********/

    var _saleTransactions = Buffer.Buffer<Types.SaleTransactionV3>(0);
    var _salesSettlements = TrieMap.TrieMap<Types.Address, Types.SaleV3>(Text.equal, Text.hash);
    var _failedSales = Buffer.Buffer<Types.SaleV3>(0);
    var _tokensForSale = Buffer.Buffer<Types.TokenIndex>(0);
    var _whitelistSpots = TrieMap.TrieMap<Types.WhitelistSpotId, Types.RemainingSpots>(Text.equal, Text.hash);
    var _saleCountByLedger = TrieMap.TrieMap<Principal, Nat>(Principal.equal, Principal.hash);
    var _saleVolumeByLedger = TrieMap.TrieMap<Principal, Nat>(Principal.equal, Principal.hash);
    var _totalToSell = 0 : Nat;
    var _nextSubAccount = 0 : Nat;

    public func getChunkCount(chunkSize : Nat) : Nat {
      var count : Nat = _saleTransactions.size() / chunkSize;
      if (_saleTransactions.size() % chunkSize != 0) {
        count += 1;
      };
      Nat.max(1, count);
    };

    public func toStableChunk(chunkSize : Nat, chunkIndex : Nat) : Types.StableChunk {
      let start = Nat.min(_saleTransactions.size(), chunkSize * chunkIndex);
      let count = Nat.min(chunkSize, _saleTransactions.size() - start);
      let saleTransactionChunk = if (_saleTransactions.size() == 0 or count == 0) {
        [];
      } else {
        Buffer.toArray(Buffer.subBuffer(_saleTransactions, start, count));
      };

      if (chunkIndex == 0) {
        ?#v3({
          saleTransactionCount = _saleTransactions.size();
          saleTransactionChunk;
          salesSettlements = Iter.toArray(_salesSettlements.entries());
          failedSales = Buffer.toArray(_failedSales);
          tokensForSale = Buffer.toArray(_tokensForSale);
          whitelistSpots = Iter.toArray(Iter.sort<(Types.WhitelistSpotId, Types.RemainingSpots)>(_whitelistSpots.entries(), func(a, b) {
            return Text.compare(a.0, b.0);
          }));
          saleCountByLedger = Iter.toArray(_saleCountByLedger.entries());
          saleVolumeByLedger = Iter.toArray(_saleVolumeByLedger.entries());
          totalToSell = _totalToSell;
          nextSubAccount = _nextSubAccount;
        });
      } else if (chunkIndex < getChunkCount(chunkSize)) {
        return ? #v3_chunk({ saleTransactionChunk });
      } else {
        null;
      };
    };

    public func loadStableChunk(chunk : Types.StableChunk) {
      switch (chunk) {
        // v2 -> v3
        case (?#v2(data)) {
          _saleTransactions := Buffer.Buffer<Types.SaleTransactionV3>(data.saleTransactionCount);

          data.saleTransactionChunk
            |> Array.map(_, func(txV2 : Types.SaleTransaction) : Types.SaleTransactionV3 {
              {
                txV2 with
                ledger = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
              };
            })
            |> Buffer.fromArray<Types.SaleTransactionV3>(_)
            |> _saleTransactions.append(_);

          _salesSettlements := data.salesSettlements.vals()
            |> Iter.map<(Types.Address, Types.Sale), (Types.Address, Types.SaleV3)>(_, func((address : Types.Address, saleV2 : Types.Sale)) : (Types.Address, Types.SaleV3) {
              (address, {
                saleV2 with
                ledger = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
              });
            })
            |> TrieMap.fromEntries(_, Text.equal, Text.hash);

          // skip: not enough data to convert
          // _failedSales := Buffer.fromArray<(Types.AccountIdentifier, Types.SubAccount)>(data.failedSales);

          _tokensForSale := Buffer.fromArray<Types.TokenIndex>(data.tokensForSale);
          _whitelistSpots := TrieMap.fromEntries(data.whitelistSpots.vals(), Text.equal, Text.hash);

          _saleCountByLedger.put(Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), data.sold);
          _saleVolumeByLedger.put(Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"), Nat64.toNat(data.soldIcp));

          _totalToSell := data.totalToSell;
          _nextSubAccount := data.nextSubAccount;
        };
        case (?#v2_chunk(data)) {
          data.saleTransactionChunk
            |> Array.map(_, func(txV2 : Types.SaleTransaction) : Types.SaleTransactionV3 {
              {
                txV2 with
                ledger = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
              };
            })
            |> Buffer.fromArray<Types.SaleTransactionV3>(_)
            |> _saleTransactions.append(_);
        };
        // v3
        case (?#v3(data)) {
          _saleTransactions := Buffer.Buffer<Types.SaleTransactionV3>(data.saleTransactionCount);
          _saleTransactions.append(Buffer.fromArray(data.saleTransactionChunk));
          _salesSettlements := TrieMap.fromEntries(data.salesSettlements.vals(), Text.equal, Text.hash);
          _failedSales := Buffer.fromArray<Types.SaleV3>(data.failedSales);
          _tokensForSale := Buffer.fromArray<Types.TokenIndex>(data.tokensForSale);
          _whitelistSpots := TrieMap.fromEntries(data.whitelistSpots.vals(), Text.equal, Text.hash);
          _saleCountByLedger := TrieMap.fromEntries(data.saleCountByLedger.vals(), Principal.equal, Principal.hash);
          _saleVolumeByLedger := TrieMap.fromEntries(data.saleVolumeByLedger.vals(), Principal.equal, Principal.hash);
          _totalToSell := data.totalToSell;
          _nextSubAccount := data.nextSubAccount;
        };
        case (?#v3_chunk(data)) {
          _saleTransactions.append(Buffer.fromArray(data.saleTransactionChunk));
        };
        case (null) {};
      };
    };

    public func grow(n : Nat) : Nat {
      let fuzz = Fuzz.Fuzz();

      for (i in Iter.range(1, n)) {
        _saleTransactions.add({
          tokens = [fuzz.nat32.random()];
          seller = fuzz.principal.randomPrincipal(10);
          price = fuzz.nat64.random();
          ledger = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
          buyer = fuzz.text.randomAlphanumeric(32);
          time = fuzz.int.randomRange(1670000000000000000, 2670000000000000000);
        });
      };

      _saleTransactions.size();
    };

    // *** ** ** ** ** ** ** ** ** * * PUBLIC INTERFACE * ** ** ** ** ** ** ** ** ** ** /

    // updates
    public func initMint(caller : Principal) : Result.Result<(), Text> {
      assert (caller == config.minter);

      if (deps._Tokens.getNextTokenId() != 0) {
        return #err("already minted");
      };

      // Mint
      mintCollection();

      // turn whitelists into TrieMap for better performance
      for (whitelist in config.whitelists.vals()) {
        for (accountId in whitelist.addresses.vals()) {
          addWhitelistSpot(whitelist, accountId);
        };
      };

      // get initial token indices (this will return all tokens as all of them are owned by "0000")
      _tokensForSale := switch (deps._Tokens.getTokensFromOwner("0000")) {
        case (?t) t;
        case (_) Buffer.Buffer<Types.TokenIndex>(0);
      };

      return #ok;
    };

    public func shuffleTokensForSale(caller : Principal) : async () {
      assert (caller == config.minter);
      switch (config.sale) {
        case (#supply(supplyCap)) {
          assert (supplyCap == _tokensForSale.size());
        };
        case (_) {};
      };
      // shuffle indices
      let seed : Blob = await Random.blob();
      Utils.shuffleBuffer(_tokensForSale, seed);
    };

    public func airdropTokens(caller : Principal) : () {
      assert (caller == config.minter and _totalToSell == 0);

      // airdrop tokens
      for (a in config.airdrop.vals()) {
        // nextTokens() updates _tokensForSale, removing consumed tokens
        deps._Tokens.transferTokenToUser(nextTokens(1)[0], a);
      };
    };

    public func airdropRemaining(caller : Principal) : () {
      assert (caller == config.minter);

      let airdrops: [(Text, Nat64)] = [
        ("801d1199b675b8a0dd565f317b11080d2f07d82ad43dfca901a000e0b4e49516", 29),
        ("30d08d681dec813f78f736f3e9d1b5894d8fb1009d88ab6ac4af0cafa2df95f9", 152),
        ("78cc47c96b4c2d318002f342fb0658ba4e4183294c0d7f7c3e6489c87d43f064", 15),
        ("81f748c10f81ddda5962ed1d9c281329e0c28d00ae8a5662ef5541fc54bbe2cc", 25),
        ("c5229e9f06f46afb01b6d3e3047efb857547256c176c1a97dc11a6b9e9ef9e49", 2),
        ("348cd556da6b943db37b7a65428eb37981b7bba915a47736b8a290436f00c455", 116),
        ("eff259c0441b0258420b618e58a789d3a158573e1a322ccb2f847c94cefc2cc6", 6),
        ("a5612c5edf4c48e02b93b2e5fbc042379b1cc70bde38eab9498b600b1e1a3061", 12),
        ("7a85630d874e4748b666ea2db9396e950fa39d5a017afd98a111335c8ec0e2da", 2),
        ("a2457c6056d20ceb73e118ebe68ac98da3a179f13c68dfaf73676211c11565c6", 253),
        ("b0b2f221e2e017b2969e97a34f72fd752ab03cfb897f31352b9c16aa17bed05c", 54),
        ("cc818989e3891971c7ab80a8106265c379b2b228d32295526e928eb4b4c9c86c", 12),
        ("96b97086ad880f1ad15c81f5ba6f35b6e7b8f02c16ca0f5ff051c91567afe8ec", 19),
        ("b31a40899bfbc84c7ba780b6780bff5f33d44ce8798084db23db1a9038f9375d", 12),
        ("1bb8b5071c2d84173fce16bd67dc5343a32aa9eca92eb36dc6b3c65c47c6fb3e", 2),
        ("7ba18d318a52d0adbde26ac3b80c3e685c26de4268218c0f879d0890e4ac5baa", 10),
        ("48f7a63d82cda1404455bda36918c64cb11978cddbe03b02eda4406a1b0ccf39", 17),
        ("09792fd06fd0c2ccc0fb0b8bdc9b2ceb1a4c84b99cc68e4c712ff975a249e425", 10),
        ("e799fccbba3788aa757f882b766484601ccec362f176a489b2c74361dd2d74f2", 19),
        ("4b69f067422ed8acb14113c44111d34e7ce04fa0e99c19fe9eafe0f9bd1d58ec", 45),
        ("ff021988dc4d4fc4b8c33c9143b781824453efb21c709854539d62e918e230f7", 12),
        ("172660628370293e3c48411f50c5f44ceaac0a47a5c8a15467d5dd6c98e2fc73", 4),
        ("30ab3ce79014b51aef970bf57c38935c53aed1f8c7ed01d806610494d4ee4729", 4),
        ("0b927d67d8316ace58aed9e8d7567d4acd4b810cb54aa02ec3a309f9b0417b24", 27),
        ("21950a5f962128579a503ca4767d08bce9a32b373300dc8e73275da6ead23f51", 31),
        ("a3c3e51da1f347966b407f0804a6f71f66210967d2bd4b32084931dcaa475e11", 8),
        ("1ed14807230b7eace71620b8594f7e9eab3a948d90ecd5ff7c01c7e0ec4b34b1", 12),
        ("47d6d19264efe042779557cc6ed17e0b2731131b667237ebe63b3678c0547882", 6),
        ("e0942307c0a44f9f0d1ba707bee6a4a47dd721f38086e8065070fc8774b8c07c", 17),
        ("07393d0fe7547f9dfdda2c879c1490dd81675a3d160f9c4940efecbdcb6c11db", 4),
        ("5dfbc31c0b33f4d08946d01f20e106934ff8aebeea777f093bd2fac7b46c1ed2", 2),
        ("19aec65306703efb421a6d46461370b6f2fc379e95f5c330402ca2f63e0613da", 4),
        ("1e08685b4a1b6754ccfb91df615fe1389686fd11a0ba411ed68f7b61c04d84f3", 4),
        ("2c32168791e5c3d6b44f12b00a38e43ac7fc72ec80f8632f9a7a4701a7b23abb", 36),
        ("dc941b958ec49c60a0286c2716732a8e03bae50d4fdc4e90709e846371a7afac", 27),
        ("1dd00785b04683871951e44ead49769f7e932a7e94304f67983b2a35cf6ff99a", 12),
        ("1b07d6d3410a112aa78be3d2279255783e2018b7dd75cdf1c9d834067a9ee214", 4),
        ("c991a7886006a1ef23db2251d25013f5ece994ce1ae20b05e2dab31e9f3e2365", 32),
        ("53df52c40028aa5f4afc4c9353306873abb2df1e7a68a39102af8403a9b6cb9f", 23),
        ("f61e15cdcaf0325bbaeb9a23a9f49d5447b33e6feee9763c2fdfe3a986142912", 2),
        ("31cb2042ed6e67429b68252490d772ec769f1bcf010dcf27b80b9edee04bb60d", 2),
        ("e5f5fabcc4101cdd4a3e9e74b6ad974a3a7cfe82aaa8d7a5b1a60f9bfd725947", 6),
        ("4f96890f17a6cde5bb8c37fe40b153474b913d0deb972f7e2dad7f9dd49cbf21", 6),
        ("cdf3277dcfd7beb5fe8cfa54f50885fb9bf59ef26774ad4e59ed6276bdba203e", 8),
        ("e5c8dc40979a1c9f8ccfee1cafeca464e12bae9302d39cc62fafdb0def17ce58", 2),
        ("47cc2920ad53367bfd602c86ef72e8876fe6324da56a49df1f66233756a6fb09", 2),
        ("7133bba50e0d75e620ae28bacd2a3b0e7a9c5e485221b838c611dc0e93689445", 8),
        ("a6e48a8939bcf9196c44f75cbc6d291b237fb1f333ff7a5d02ab8b28141085bb", 10),
        ("f7424f5ad2f280f079660adc98158c3e83fd34c3893d8e59d8738681eafd3bfb", 4),
        ("ffe590c7f3e1561a75541495348433c82ab785a78acd7156e56aef3f19afdada", 8),
        ("1eae2dc33b5a5b304b3beb1c4422461a31b6624ff41511ccd7e92e854f6c3862", 6),
        ("42506163c317331b52429d961ae3e8b0851d6262b1e069c88740b0a7cca66ec3", 6),
        ("bfa6f3d2573dd1380b922b571770efe934e119490902697fed0cc07ff233dba8", 2),
        ("0a74c5bd278d5912c15d8e090206c6d2c263f3f5bf9a19912150b0f96ba22805", 2),
        ("64beffe7f7534c27ba4973f1bce2f951248abba580b4f0e267433fa7727715bb", 6),
        ("800226c042789e5a7f03f5121f86dd6d0517fef43d8efb44e2d37c9855ca27c7", 21),
        ("f9b86e329ba93730f77b31f5514ce62609221b5f203749866b393e13a3c2b452", 6),
        ("b5e3dc1b2cb8a75bcaa9b160286f56e7b31b5805c083cc6042412f9e80cb9e19", 6),
        ("5b0d0ef3f5ceb39709e808dab2923390826fbedcc62da14085a7a641a654cdb5", 4),
        ("c80e66e1a7c0bfb36b7fe95f126298e5ad0d5086d3e011c3b9267227b702d4db", 12),
        ("acd50fe26a641d46757ae1a11fbeeb9a7c52bd40f388b060ed46d199d9d8db24", 2),
        ("100b83ac9cfdedcfbbe4f61c67cf23ce30ad617c084481f5c4555d2ecb56c2ec", 6),
        ("3b4eab4e37db807a395c6e6567d84ae1fd66a198a7e6ea0dd909ee972390f15e", 12),
        ("d415e0c7c702584bc19527bdbf5ddfa0a7b4661dc5b9982ff5aa20b9affb8531", 6),
        ("8a53c8da6de10e71591a8d86be91b6db62155bd1f8327f1509b922b407f51fdf", 2),
        ("5e48774f9a5e8df421e6cd4a9c03f1980db16de470af0a9da5d640665a8e5cac", 8),
        ("8d5f8f2d76dd4117e1ca152d4480e2a618cac22c9660ed8c931f510e814ccff7", 6),
        ("158a34edd5e5d7cd9911848ff5a740f1c6d6de12a28f418c859fd468e052f39f", 2),
        ("3b08035c106d20549fe088c05fba318a4c9d83f2b5323e8d26b465adb4a9dd6e", 4),
        ("f14889b2b04595b5705c44741752d91e54245f41c20682717e3fbe395ce982e8", 2),
        ("90d9c31a37abea26b61b2353196acf3232fb11a7c99a52b3dfc587300321dfdf", 6),
        ("4a9e28c7fe343d82d2d31fcb17015d600466091e95aaf716d943e2f5e5f08d6d", 8),
        ("f771bff3e411c13f5fad663330f0f6ca63e72f945fd879d74fb860c84835a00c", 2),
        ("1939ae0d38e14a78290c822c2d98553fae16fff58fbc8dc48c399d7ae169a817", 12),
        ("6bf8f02b901d9989017212ad42d20c546e10bd81fda6d7ca05802c411da69187", 2),
        ("60f6e3719d6052fdcd7e3cc1649c69a6f093fd4361e0e177606c24b72ba273f6", 2),
        ("89608f311b29030f940ca52e77c170b1c68b7abf2d62a319e2beb241307470e4", 6),
        ("2d00c735893f9746b8f42733f7beee397148becc06408788479ca5d4cef8a660", 25),
        ("9789252b46b1f073e6eb24c9543984a83d84506ac8c1785e14ee7581fc6d5928", 4),
        ("acc899f0d9ea43ede30547e302e5fca7d0cdf6b1e087f19f496c7dfc55fe175d", 4),
        ("26b8e2beb1bb9f6de4d9594064beb41778367babbd5c97504643f62f141ce7e0", 6),
        ("e09c8d5ad3bc4c94546df0124ca26ad1439ed05dba322389fe3b222bcfff3136", 2),
        ("a11c8b592760cc65e980c6f2d0e8abcc78477450f1fe3b7ade0a5a9eede059ba", 6),
        ("6930ca7d16a632e271197e5e42ec523c07e2ae4eef5a4bb97ee795d0a9b6a2f7", 2),
        ("dbb29a9d73a8426b38b954d317a9f5b72bc41406b2efe609e458a0e97fadea20", 12),
        ("df531e38324b8aed256e529eff465fdba43058c29500bb93cced66daa7db3e7c", 21),
        ("18a7031bbad9ad8f5db596946c893c660abd4d431f3bd9ad3ec287cd000cbeb1", 4),
        ("8fa0166bcdb1b7f1de637b88661e8303eeebb023666c8893dfa260afaee9d524", 6),
        ("05d03259d7a1a4be9f927d7360cbea38ce44b81b236fb97427d24ff4f1da62c6", 2),
        ("7688d27a065b31f318e03d8a45e0c22edd4a1eecfd5c02b15ed768e00b13a5b8", 4),
        ("68d1a5f0ae8911c33c16a3785856b7490e95e2a1209c30942a07854ddfefc940", 2),
        ("8410388da230b6a8c6d62a6a1f245de22b1d4191e008c6b748d9e456031159cb", 2),
        ("24d40a7e6ee1e84e3680df3e8f7e938fae3d24c71eb95e231fe91a604aeba475", 4),
        ("03849f6af2b01270c771c070484324631690150f9afb22485bfe8489051ff3b9", 6),
        ("a02fdf024ecd93fc426bb09db88abd5d93985f3b6022939db0ac76efec3067ad", 4),
        ("f859a28b9a145aa220a663a3651781b11e6cb0eb91dea4102898a691797d6e02", 6),
        ("91d8f0381db3af29d386f220ae1e5cc2ed7fd468597e0a65a633f72947bc79c0", 4),
        ("502062492ecee5b58908839ba094bbd67fa46d3447d4c82b376f09c296ff7e84", 2),
        ("092122204430fe95043eebb43bc63c14500eb7bc6ef72ba578d8e99e7d7f01ee", 2),
        ("47c8d3d732147faeb8261e1aa2c7b303c2003b1d497aac25d3d0be7724087d9f", 2),
        ("59fe1ba5f84295c1b737d0aa8b6211699f55896d62a0e141cd54b3ff5bea4e85", 6),
        ("d877c19bc0bf1bbe76f19da30eb1cc58646f6d055f73331d5678f6a1705466e9", 6),
        ("a27b065628d4c85665319f73e2eb2a53f79dccfad96d9c256c98c73e2d284c50", 2),
        ("8b5965ade4481f65323d495f693148f7ae27d48ea3d620f2a244be42b6f6415d", 2),
        ("74e470c0cd4bd98d9e3a3f163719ef83e15f48d14ee91546f846a3f9f9ae0d04", 6),
        ("46528c33c255652663bdf305bf3dde5064df75e0f7a84ca7d5b1f0334ba71f9c", 2),
        ("68134373bed8f81703485987e20a5f87fa5f9e5a1eb1436f905a1bf34e893dcf", 2),
        ("1f46afe1899621a0fd8e0e5e7847ac0956003383b91a6c168676c5dcabdfc4e2", 2),
        ("fab0b6d63d89cae616c45b75c2388606d5d07898522200950a00dc47f2e7b0db", 2),
        ("a652a7a396f2905e5240f673f7c34b87975642ff4a731a27bb460f4dbb3977ef", 6),
        ("7d00b55bdd464dd9b55cfa96ed1a61cc3ed0736f81e91f9875384dbe91a70256", 2),
        ("d7a00d6a330053ff80ee2c94d1aff382554b2529b219580fc0abbc38b2f836ef", 2),
        ("a17fec45df36c7cc45a13a2754e2ab7c7a35a179f1511d44b30753646de69959", 2),
        ("d213b62fa54352a3032dfdc27a7e6b6f660f1663e6d60a269afe009bad10e975", 2),
        ("2b3a9f9fedd9fda1072519e94e86147acfe0f20da2382579d0a3d36d0047aa0c", 2),
        ("c6c622bb26ca74158caa208f79b6ac721b83fed014eef51a39d69cec4ef0411b", 2),
        ("3496ccc519f4f67acbed07b7ab74757da29f720341f14506db945b64c34c5c43", 2),
        ("79ee5e1df165a3ce1209d0547ff7f098f9f8591f87b159191435929af0fc50fd", 2),
        ("443c1e72487144e30505eb50f1134ea209bb57cb038f26bee139cf746ed90868", 2),
      ];

      for ((accountId, count) in airdrops.vals()) {
        let tokens = nextTokens(count);

        for (token in tokens.vals()) {
          deps._Tokens.transferTokenToUser(token, accountId);
        };
      };
    };

    public func enableSale(caller : Principal) : Nat {
      assert (caller == config.minter and _totalToSell == 0);
      _totalToSell := _tokensForSale.size();
      _tokensForSale.size();
    };

    public func reserve(caller : Principal, address : Types.Address, ledger : Principal) : Result.Result<(Types.Address, Nat64), Text> {
      switch (config.sale) {
        case (#duration(duration)) {
          if (Time.now() > config.publicSaleStart + Utils.toNanos(duration)) {
            return #err("The sale has ended");
          };
        };
        case (_) {};
      };

      // check if caller is owner of the address
      let addressAccountRes = Account.fromText(address);
      switch (addressAccountRes) {
        case (#ok(addressAccount)) {
          if (caller != addressAccount.owner) {
            return #err("Reserve can only be called by the owner of the address");
          };
        };
        case (#err(_)) {
          return #err("Invalid address. Please make sure the address is a valid principal");
        };
      };

      // check if the ledger is allowed
      if (not _isLedgerAllowed(ledger)) {
        return #err("This ledger is not allowed");
      };

      let inPendingWhitelist = Option.isSome(getEligibleWhitelist(address, true));
      let inOngoingWhitelist = Option.isSome(getEligibleWhitelist(address, false));

      if (Time.now() < config.publicSaleStart) {
        if (inPendingWhitelist and not inOngoingWhitelist) {
          return #err("The sale has not started yet");
        } else if (not isWhitelisted(address)) {
          return #err("The public sale has not started yet");
        };
      };

      if (availableTokensForLedger(ledger) == 0) {
        return #err("No more NFTs available right now!");
      };

      let price = getAddressPrice(address, ledger);
      let subaccount = getNextSubAccount();
      let paymentAddress : Types.Address = Account.toText({ owner = config.canister; subaccount = ?Blob.fromArray(subaccount) });

      // we only reserve the tokens here, they deducted from the available tokens
      // after payment. otherwise someone could stall the sale by reserving all
      // the tokens without paying for them
      let tokens : [Types.TokenIndex] = tempNextTokens(1);
      _salesSettlements.put(
        paymentAddress,
        {
          tokens = tokens;
          price = price;
          ledger = ledger;
          subaccount = subaccount;
          buyer = address;
          expires = Time.now() + Utils.toNanos(Option.get(config.escrowDelay, #minutes(2)));
          whitelistName = switch (getEligibleWhitelist(address, false)) {
            case (?whitelist) ?whitelist.name;
            case (null) null;
          };
        },
      );

      // remove whitelist spot if one time only
      switch (getEligibleWhitelist(address, false)) {
        case (?whitelist) {
          if (whitelist.oneTimeOnly) {
            removeWhitelistSpot(whitelist, Utils.toAccountId(address));
          };
        };
        case (null) {};
      };

      #ok((paymentAddress, price));
    };

    public func addWhitelists() {
      for (whitelist in config.whitelists.vals()) {
        for (accountId in whitelist.addresses.vals()) {
          addWhitelistSpot(whitelist, accountId);
        };
      };
    };

    public func retrieve(caller : Principal, paymentAddress : Types.Address) : async* Result.Result<(), Text> {
      var settlement = switch (_salesSettlements.get(paymentAddress)) {
        case (?settlement) { settlement };
        case (null) {
          return #err("Nothing to settle");
        };
      };
      let ledger = settlement.ledger;

      let paymentAccount : ICRC1.Account = switch (Account.fromText(paymentAddress)) {
        case (#ok(account)) account;
        case (#err(_)) {
          // this should never happen because payment accounts are always created from within the canister which should guarantee that they are valid
          _salesSettlements.delete(paymentAddress);
          return #err("Failed to decode payment address");
        };
      };

      let ledgerActor = actor(Principal.toText(ledger)) : ICRC1.Service;
      let ledgerFee = await* _getLedgerFee(ledger);
      let balance = Nat64.fromNat(await ledgerActor.icrc1_balance_of(paymentAccount));

      // because of the await above, we check again if there is a settlement available for the paymentAddress
      settlement := switch (_salesSettlements.get(paymentAddress)) {
        case (?settlement) { settlement };
        case (null) {
          return #err("Nothing to settle");
        };
      };

      if (settlement.tokens.size() == 0) {
        _salesSettlements.delete(paymentAddress);
        return #err("Nothing tokens to settle for");
      };

      if (balance >= settlement.price) {
        if (settlement.tokens.size() > availableTokensForLedger(ledger)) {
          // Issue refund if not enough NFTs available
          deps._Disburser.addDisbursement({
            ledger = ledger;
            to = settlement.buyer;
            fromSubaccount = settlement.subaccount;
            amount = balance - ledgerFee;
            tokenIndex = 0;
          });
          _salesSettlements.delete(paymentAddress);

          return #err("Not enough NFTs - a refund will be sent automatically very soon");
        };

        let tokens = nextTokens(Nat64.fromNat(settlement.tokens.size()));

        // transfer tokens to buyer
        for (token in tokens.vals()) {
          deps._Tokens.transferTokenToUser(token, Utils.toAccountId(settlement.buyer));
        };

        _saleTransactions.add({
          tokens = tokens;
          seller = config.canister;
          price = settlement.price;
          ledger = settlement.ledger;
          buyer = settlement.buyer;
          time = Time.now();
        });

        _saleCountByLedger.put(settlement.ledger, Option.get(_saleCountByLedger.get(settlement.ledger), 0) + tokens.size());
        _saleVolumeByLedger.put(settlement.ledger, Option.get(_saleVolumeByLedger.get(settlement.ledger), 0) + Nat64.toNat(settlement.price));

        _salesSettlements.delete(paymentAddress);
        let event : Root.IndefiniteEvent = {
          operation = "mint";
          details = [
            ("to", #Text(settlement.buyer)),
            ("price_decimals", #U64(8)),
            ("price_canister", #Principal(ledger)),
            ("price", #U64(settlement.price)),
            // there can only be one token in tokens due to the reserve function
            ("token_id", #Text(Utils.indexToIdentifier(settlement.tokens[0], config.canister))),
          ];
          caller;
        };
        ignore deps._Cap.insert(event);
        // Payout
        // remove total transaction fee from balance to be splitted
        let bal : Nat64 = balance - (ledgerFee * Nat64.fromNat(config.salesDistribution.size()));

        // disbursement sales
        for (f in config.salesDistribution.vals()) {
          var _fee : Nat64 = bal * f.1 / 100000;
          deps._Disburser.addDisbursement({
            ledger = ledger;
            to = f.0;
            fromSubaccount = settlement.subaccount;
            amount = _fee;
            tokenIndex = 0;
          });
        };
        return #ok();
      } else {
        // if the settlement expired and they still didn't send the full amount, we add them to failedSales
        if (settlement.expires < Time.now()) {
          _failedSales.add(settlement);
          _salesSettlements.delete(paymentAddress);

          // add back to whitelist if one time only
          switch (settlement.whitelistName) {
            case (?whitelistName) {
              for (whitelist in config.whitelists.vals()) {
                if (whitelist.name == whitelistName and whitelist.oneTimeOnly) {
                  addWhitelistSpot(whitelist, Utils.toAccountId(settlement.buyer));
                };
              };
            };
            case (_) {};
          };
          return #err("Expired");
        } else {
          return #err("Insufficient funds sent");
        };
      };
    };

    public func cronSalesSettlements(caller : Principal) : async* () {
      // _saleSattlements can potentially be really big, we have to make sure
      // we dont get out of cycles error or error that outgoing calls queue is full.
      // This is done by adding the await statement.
      // For every message the max cycles is reset
      label settleLoop while (true) {
        switch (getExpiredSalesSettlements().entries().next()) {
          case (?(paymentAddress, settlement)) {
            try {
              ignore (await* retrieve(caller, paymentAddress));
            } catch (e) {
              break settleLoop;
            };
          };
          case null break settleLoop;
        };
      };
    };

    public func cronFailedSales() : async* () {
      label failedSalesLoop while (true) {
        let last = _failedSales.removeLast();
        switch (last) {
          case (?failedSale) {
            try {
              // check if subaccount holds tokens
              let ledgerActor = actor(Principal.toText(failedSale.ledger)) : ICRC1.Service;
              let ledgerFee = await* _getLedgerFee(failedSale.ledger);

              let balance = Nat64.fromNat(await ledgerActor.icrc1_balance_of({
                owner = config.canister;
                subaccount = ?Blob.fromArray(failedSale.subaccount);
              }));

              if (balance > ledgerFee) {
                let buyerAccount : ICRC1.Account = switch (Account.fromText(failedSale.buyer)) {
                  case (#ok(account)) account;
                  case (#err(_)) {
                    // this should never happen because payment accounts are always created from within the canister which should guarantee that they are valid
                    continue failedSalesLoop;
                  };
                };

                var res = await ledgerActor.icrc1_transfer({
                  memo = null;
                  amount = Nat64.toNat(balance - ledgerFee);
                  fee = ?Nat64.toNat(ledgerFee);
                  from_subaccount = ?Blob.fromArray(failedSale.subaccount);
                  to = buyerAccount;
                  created_at_time = null;
                });

                switch (res) {
                  case (#Ok(bh)) {};
                  case (#Err(_)) {
                    // if the transaction fails for some reason, we add it back to the Buffer
                    _failedSales.add(failedSale);
                    break failedSalesLoop;
                  };
                };
              };
            } catch (e) {
              // if the transaction fails for some reason, we add it back to the Buffer
              _failedSales.add(failedSale);
              break failedSalesLoop;
            };
          };
          case (null) {
            break failedSalesLoop;
          };
        };
      };
    };

    public func getNextSubAccount() : Types.SubAccount {
      var _saOffset = 4294967296;
      _nextSubAccount += 1;
      return Utils.natToSubAccount(_saOffset +_nextSubAccount);
    };

    // queries
    public func salesSettlements() : [(Types.Address, Types.SaleV3)] {
      Iter.toArray(_salesSettlements.entries());
    };

    public func getUserSettlements(principal : Principal) : [(Types.Address, Types.SaleV3)] {
      let ptext = Principal.toText(principal);
      _salesSettlements.entries()
        |> Iter.filter(_, func((address, settlement) : (Types.Address, Types.SaleV3)) : Bool {
          settlement.buyer == ptext;
        })
        |> Iter.toArray(_);
    };

    public func getUserFailedSales(principal : Principal) : [Types.SaleV3] {
      let ptext = Principal.toText(principal);
      _failedSales.vals()
        |> Iter.filter(_, func(sale : Types.SaleV3) : Bool {
          sale.buyer == ptext;
        })
        |> Iter.toArray(_);
    };

    public func failedSales() : [Types.SaleV3] {
      Buffer.toArray(_failedSales);
    };

    public func saleTransactions() : [Types.SaleTransactionV3] {
      Buffer.toArray(_saleTransactions);
    };

    public func getSold() : Nat {
      var totalSold = 0;
      for (sold in _saleCountByLedger.vals()) {
        totalSold += sold;
      };
      totalSold;
    };

    public func soldIcp() : Nat64 {
      Nat64.fromNat(Option.get(_saleVolumeByLedger.get(Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")), 0));
    };

    public func getTotalToSell() : Nat {
      _totalToSell;
    };

    public func salesSettings(address : Types.Address) : Types.SaleSettingsV3 {
      var startTime = config.publicSaleStart;
      var endTime : Time.Time = 0;

      switch (config.sale) {
        case (#duration(duration)) {
          endTime := config.publicSaleStart + Utils.toNanos(duration);
        };
        case (_) {};
      };

      // for whitelisted user return nearest and cheapest slot start time
      switch (getEligibleWhitelist(address, true)) {
        case (?whitelist) {
          startTime := whitelist.startTime;
          endTime := Option.get(whitelist.endTime, 0);
        };
        case (_) {};
      };

      let icpPriceInfoOpt = Array.find(config.salePrices, func(p : RootTypes.PriceInfo) : Bool {
        p.ledger == Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai")
      });
      let icpPriceOpt = Option.map(icpPriceInfoOpt, func(p : RootTypes.PriceInfo) : Nat64 { p.price; });

      return {
        price = getAddressPrice(address, Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai"));
        salePrice = Option.get(icpPriceOpt, 0 : Nat64);
        prices = getAddressPrices(address);
        salePrices = config.salePrices;
        remaining = availableTokens();
        remainingByLedger = Array.map<RootTypes.PriceInfoWithLimit, (Principal, Nat)>(config.salePrices, func(p : RootTypes.PriceInfo) {
          (p.ledger, availableTokensForLedger(p.ledger));
        });
        sold = getSold();
        totalToSell = _totalToSell;
        startTime = startTime;
        endTime = endTime;
        whitelistTime = config.publicSaleStart;
        whitelist = isWhitelisted(address);
        openEdition = openEdition;
      } : Types.SaleSettingsV3;
    };

    /*******************
    * INTERNAL METHODS *
    *******************/

    // getters & setters
    public func availableTokens() : Nat {
      if (openEdition) {
        return 1;
      };
      _tokensForSale.size();
    };

    public func availableTokensForLedger(ledger : Principal) : Nat {
      if (openEdition) {
        return 1;
      };

      let ?priceInfo = Array.find(config.salePrices, func(p : RootTypes.PriceInfoWithLimit) : Bool {
        p.ledger == ledger
      }) else {
        return 0;
      };

      switch (priceInfo.limit) {
        case (?limit) {
          let soldCount = Option.get(_saleCountByLedger.get(ledger), 0);
          let remaining = limit - soldCount : Nat;
          Nat.min(remaining, _tokensForSale.size());
        };
        case (null) {
          _tokensForSale.size();
        };
      };
    };

    // internals
    func tempNextTokens(qty : Nat64) : [Types.TokenIndex] {
      Array.freeze(Array.init<Types.TokenIndex>(Nat64.toNat(qty), 0));
    };

    func getAddressPrice(address : Types.Address, ledger : Principal) : Nat64 {
      let prices = getAddressPrices(address);
      let priceInfoOpt = Array.find(prices, func(p : RootTypes.PriceInfo) : Bool {
        p.ledger == ledger
      });
      switch (priceInfoOpt) {
        case (?priceInfo) priceInfo.price;
        case (null) {
          Debug.trap("Price not found for ledger " # Principal.toText(ledger) # " and address " # address);
        };
      };
    };

    func getAddressPrices(address : Types.Address) : [RootTypes.PriceInfo] {
      // dutch auction (only ICP ledger is supported for now)
      switch (config.dutchAuction) {
        case (?dutchAuction) {
          // dutch auction for everyone
          let everyone = dutchAuction.target == #everyone;
          // dutch auction for whitelist (tier price is ignored), then salePrice for public sale
          let whitelist = dutchAuction.target == #whitelist and isWhitelisted(address);
          // tier price for whitelist, then dutch auction for public sale
          let publicSale = dutchAuction.target == #publicSale and not isWhitelisted(address);

          if (everyone or whitelist or publicSale) {
            return [{
              ledger = Principal.fromText("ryjl3-tyaaa-aaaaa-aaaba-cai");
              price = getCurrentDutchAuctionPrice(dutchAuction);
            }];
          };
        };
        case (null) {};
      };

      // we have to make sure to only return prices that are available in the current whitelist slot
      // if i had a wl in the first slot, but now we are in slot 2, i should not be able to buy at the price of slot 1

      // this method assumes the wl prices are added in ascending order, so the cheapest wl price in the earliest slot
      // is always the first one.
      switch (getEligibleWhitelist(address, true)) {
        case (?whitelist) {
          return whitelist.prices;
        };
        case (_) {};
      };

      return config.salePrices;
    };

    func getCurrentDutchAuctionPrice(dutchAuction : RootTypes.DutchAuction) : Nat64 {
      let start = if (dutchAuction.target == #publicSale or config.whitelists.size() == 0) {
        config.publicSaleStart;
      } else {
        config.whitelists[0].startTime;
      };
      let timeSinceStart : Int = Time.now() - start; // how many nano seconds passed since the auction began
      // in the event that this function is called before the auction has started, return the starting price
      if (timeSinceStart < 0) {
        return dutchAuction.startPrice;
      };
      let priceInterval = timeSinceStart / dutchAuction.interval; // how many intervals passed since the auction began
      // what is the discount from the start price in this interval
      let discount = Nat64.fromIntWrap(priceInterval) * dutchAuction.intervalPriceDrop;
      // to prevent trapping, we check if the start price is bigger than the discount
      if (dutchAuction.startPrice > discount) {
        return dutchAuction.startPrice - discount;
      } else {
        return dutchAuction.reservePrice;
      };
    };

    func nextTokens(qty : Nat64) : [Types.TokenIndex] {
      if (openEdition) {
        deps._Tokens.mintNextToken();
        _tokensForSale := switch (deps._Tokens.getTokensFromOwner("0000")) {
          case (?t) t;
          case (_) Buffer.Buffer<Types.TokenIndex>(0);
        };
      };

      if (_tokensForSale.size() >= Nat64.toNat(qty)) {
        var ret : List.List<Types.TokenIndex> = List.nil();
        while (List.size(ret) < Nat64.toNat(qty)) {
          switch (_tokensForSale.removeLast()) {
            case (?token) {
              ret := List.push(token, ret);
            };
            case _ return [];
          };
        };
        List.toArray(ret);
      } else {
        Debug.trap("Not enough tokens to sell");
      };
    };

    func getWhitelistSpotId(whitelist : Types.Whitelist, accountId : Types.AccountIdentifier) : Types.WhitelistSpotId {
      whitelist.name # ":" # accountId;
    };

    func addWhitelistSpot(whitelist : Types.Whitelist, accountId : Types.AccountIdentifier) {
      let remainingSpots = Option.get(_whitelistSpots.get(getWhitelistSpotId(whitelist, accountId)), 0);
      _whitelistSpots.put(getWhitelistSpotId(whitelist, accountId), remainingSpots + 1);
    };

    func removeWhitelistSpot(whitelist : Types.Whitelist, accountId : Types.AccountIdentifier) {
      let remainingSpots = Option.get(_whitelistSpots.get(getWhitelistSpotId(whitelist, accountId)), 0);
      if (remainingSpots > 0) {
        _whitelistSpots.put(getWhitelistSpotId(whitelist, accountId), remainingSpots - 1);
      } else {
        _whitelistSpots.delete(getWhitelistSpotId(whitelist, accountId));
      };
    };

    // get a whitelist that has started, hasn't expired, and hasn't been used by an address
    func getEligibleWhitelist(address : Types.Address, allowNotStarted : Bool) : ?Types.Whitelist {
      let accountId = Utils.toAccountId(address);

      for (whitelist in config.whitelists.vals()) {
        let spotId = getWhitelistSpotId(whitelist, accountId);
        let remainingSpots = Option.get(_whitelistSpots.get(spotId), 0);
        let whitelistStarted = Time.now() >= whitelist.startTime;
        let endTime = Option.get(whitelist.endTime, 0);
        let whitelistNotExpired = Time.now() <= endTime or endTime == 0;

        if (remainingSpots > 0 and (allowNotStarted or whitelistStarted) and whitelistNotExpired) {
          return ?whitelist;
        };
      };
      return null;
    };

    // this method is time sensitive now and only returns true, iff the address is whitelist in the current slot
    func isWhitelisted(address : Types.Address) : Bool {
      Option.isSome(getEligibleWhitelist(address, false));
    };

    func mintCollection() {
      deps._Tokens.mintCollection();
    };

    func getExpiredSalesSettlements() : TrieMap.TrieMap<Types.Address, Types.SaleV3> {
      TrieMap.mapFilter<Types.Address, Types.SaleV3, Types.SaleV3>(
        _salesSettlements,
        Text.equal,
        Text.hash,
        func(a : (Types.Address, Types.SaleV3)) : ?Types.SaleV3 {
          switch (a.1.expires < Time.now()) {
            case (true) {
              ?a.1;
            };
            case (false) {
              null;
            };
          };
        },
      );
    };

    func _isLedgerAllowed(ledger : Principal) : Bool {
      Array.find(config.salePrices, func(p : RootTypes.PriceInfo) : Bool {
        p.ledger == ledger
      }) != null;
    };

    let _feeByLedger = TrieMap.TrieMap<Principal, Nat64>(Principal.equal, Principal.hash);

    func _getLedgerFee(ledger : Principal) : async* Nat64 {
      switch (_feeByLedger.get(ledger)) {
        case (?fee) {
          fee;
        };
        case (null) {
          let ledgerActor = actor(Principal.toText(ledger)) : ICRC1.Service;
          let fee = Nat64.fromNat(await ledgerActor.icrc1_fee());
          _feeByLedger.put(ledger, fee);
          fee;
        };
      };
    };
  };
};
