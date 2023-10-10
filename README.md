## Extração RREO - Anexo 3 - Tesouro Nacional

Script que extrai dados da [API](https://apidatalake.tesouro.gov.br/docs/siconfi) do STN, retornando um arquivo *parquet* que consolida todos os dados da pasta src/rreo03/dados/.

Esse *parquet* que será usado no *FinData*.

### Como usar

Digamos que queremos extrair dados do ano de 2023, segundo bimestre.

Então na linha de comando:

```bash
# ativa ambiente virtual
$ source ve/bin/activate  
(ve) $ python src/rreo03/extracao.py --ano_ext=[2023] --bimestre_ext=[2]
```

Caso esse parâmetro ano não seja passado, apenas o ano corrente é extraído.

Caso bimestre não seja passado, apenas o primeiro bimestre será extraído.

Para passar um conjunto de bimestres, é necessário *forçar* um [argumento simples](https://stackoverflow.com/questions/56825679/how-to-pass-a-list-array-as-argument-to-python-fire), **passando os valores sem espaços**:

```bash
(ve) $ python src/rreo03/extracao.py --ano_ext=[2023] --bimestre_ext=[2,3]
```

Há o parâmetro *formato* que aceita que o arquivo final seja em *csv*, além do padrão *parquet*:

```bash
(ve) $ python src/rreo03/extracao.py --ano_ext=[2020] --bimestre_ext=[6] --formato='csv'
```

Como não se passa mais de um valor nesse parâmentro, é desnecessário o uso de [].

Por fim, o parâmetro *ano_agreg* que produz o arquivo final no intervalo de 2015 até o ano informado.

Caso eu queira gerar um dataset até 2022:

```bash
(ve) $ python src/rreo03/extracao.py --ano_ext=[2023] --bimestre_ext=[2]
```
