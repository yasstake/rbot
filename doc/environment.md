## 取引所アクセスキーの設定方法

### 設定ファイル名
ホームディレクトリの`.env/rusty-bot/`ディレクトリに`{取引所名}.env`という名前のファイルを作成し、アクセスキーを設定します。テストネットを使う場合には`{取引所名}_TEST.env`というファイル名前にします。

例
```
.env/rusty-bot/
└── BITBANK.env
    BINANCE.env
    BINANCE_TEST.env
```

### 設定ファイル中身

APIキーとAPIシークレットを以下のように設定してください


```{取引所名.env}
API_KEY=取引所から提供されるAPIキー
API_SECRET=取引所から提供されるAPIシークレット
```


