# Import dependencies
import json
import time
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from urllib.error import HTTPError

class BrasilIO:

    base_url = "https://api.brasil.io/v1/"
    REQUEST_DELAY = 5  # Adjust the delay as needed

    def __init__(self, user_agent=None, auth_token=None):
        """
        Inicializa a instância BrasilIO.

        Args:
            user_agent (str): Identificador do usuário para a requisição na API.
            auth_token (str): Token de autenticação para a requisição na API.
        """
        self.__user_agent = user_agent
        self.__auth_token = auth_token

    def headers(self, api=True):
        """
        Retorna os cabeçalhos HTTP necessários para a requisição.

        Args:
            api (bool): Se True, retorna cabeçalhos para requisição na API, caso contrário, retorna cabeçalhos para download de arquivos.

        Returns:
            dict: Dicionário contendo os cabeçalhos HTTP.
        """
        if api:
            return {
                "User-Agent": f"{self.__user_agent}",
                "Authorization": f"Token {self.__auth_token}"
            }
        else:
            return {
                "User-Agent": "python-urllib/brasilio-client-0.1.0",
            }

    def api_request(self, path, query_string=None):
        url = urljoin(self.base_url, path)
        if query_string:
            url += "?" + query_string
        request = Request(url, headers=self.headers(api=True))

        while True:
            try:
                response = urlopen(request)
                return json.load(response)
            except HTTPError as e:
                if e.code == 429:
                    print("Too many requests. Waiting...")
                    time.sleep(self.REQUEST_DELAY)
                else:
                    raise e

    def data(self, dataset_slug, table_name, filters=None):
        """
        Obtém os dados de um dataset.

        Args:
            dataset_slug (str): Slug do dataset.
            table_name (str): Nome da tabela no dataset.
            filters (dict): Filtros para aplicar na requisição.

        Yields:
            dict: Um dicionário representando uma linha de dados.
        """
        url = f"dataset/{dataset_slug}/{table_name}/data/"
        filters = filters or {}
        filters["page"] = 1

        while True:
            query_string = "&".join([f"{k}={v}" for k, v in filters.items()])
            response = self.api_request(url, query_string)
            next_page = response.get("next", None)
            for row in response["results"]:
                yield row
            if not next_page:
                break
            filters = {}
            url = next_page

    def download(self, dataset, table_name, file_path):
        """
        Baixa um arquivo do dataset.

        Args:
            dataset (str): Slug do dataset.
            table_name (str): Nome da tabela no dataset.
            file_path (str): Caminho do arquivo onde o conteúdo será salvo.
        """
        url = f"https://data.brasil.io/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers(api=False))
        with urlopen(request) as response, open(file_path, "wb") as f:
            while True:
                chunk = response.read(4096)
                if not chunk:
                    break
                f.write(chunk)

"""
if __name__ == "__main__":
    user_agent = "brunojaciel"
    auth_token = "972fcc73b93d75aa832018e4023ea663ef98c69d"

    api = BrasilIO(user_agent, auth_token)

    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Accumulate rows in a list
    data = []

    # Download do arquivo completo
    file_path = f"{dataset_slug}_{table_name}.csv.gz"
    api.download(dataset_slug, table_name, file_path)
    i = 0
    # Leitura do arquivo em memória
    with gzip.open(file_path, mode="rt") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Insert UUID for each row
            row['uuid'] = uuid.uuid4()
            data.append(row)

    print(data)
"""
