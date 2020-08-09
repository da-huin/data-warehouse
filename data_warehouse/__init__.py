import random
import csv
from io import StringIO
import types
import json
import re
import boto3
import datetime
import os
import time
import easy_s3

class DataWarehouse():

    def __init__(self, bucket_name, table_name="", aws_access_key_id=None, aws_secret_access_key=None, region_name=None, warehouse_name="warehouse"):
        """
        1. 저장
            * raw_save
            * discovery_save
        2. 불러오기
            * raw_load
            * discovery_load
        3. 리스트 불러오기
            * raw_list
            * discover_list
        4. 아테나로 데이터 불러오기
            * load_with_athena_query
        """

        self.warehouse_name = warehouse_name
        self.bucket_name = bucket_name
        self.table_name = table_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._region_name = region_name
        self._es_handler = easy_s3.EasyS3(bucket_name, self.warehouse_name, self._region_name, self._aws_access_key_id, self._aws_secret_access_key)

        self._athena_client = None


    # 실제 사용하는 기능들
    def raw_save(self, user_path, value, meta={}, use_ymd=True, table_name="", table_kind="general", ymd=""):
        """
        raw 저장소에 저장합니다.
        """
        return self._save_worker("raw", table_kind, user_path,
                                 value, meta, use_ymd, table_name, ymd)


    def discovery_save(self, user_path, value, meta={}, use_ymd=True, table_name="", table_kind="general", ymd=""):
        """
        discovery 저장소에 저장합니다.
        """           
        return self._save_worker("discovery", table_kind, user_path,
                                 value, meta, use_ymd, table_name, ymd)

    def raw_load(self, user_path, table_name="", ymd="", is_meta=False, table_kind="general"):
        """
        raw 저장소에서 불러옵니다.
        """             
        return self._load_worker("raw", table_name, user_path, table_kind, ymd, is_meta)


    def discovery_load(self, user_path, table_name="", ymd="", is_meta=False, table_kind="general"):
        """
        discovery 저장소에서 불러옵니다.
        """          
        return self._load_worker("discovery", table_name, user_path, table_kind, ymd, is_meta)
        

    def raw_list(self, table_name="", user_dir="", ymd="", table_kind="general", load=False, is_meta=False, includes=[], is_direoctry=False):
        """
        raw 저장소에서 리스트합니다.
        """          
        return self._list_worker("raw", table_kind, table_name, user_dir, ymd, load, is_meta, includes, is_direoctry)

    def discovery_list(self, table_name="", user_dir="", ymd="", table_kind="general", load=False, is_meta=False, includes=[], is_direoctry=False):
        """
        discovery 저장소에서 리스트합니다.
        """           
        return self._list_worker("discovery", table_kind, table_name, user_dir, ymd, load, is_meta, includes, is_direoctry)

    def load_with_full_path(self, full_path):
        return self._es_handler._load_file(full_path)

    def load_meta_with_full_path(self, full_path):
        return self._es_handler._load_file(self._get_meta_path_with_full_path(full_path))

    def _get_meta_path_with_full_path(self, full_path):
        splited = full_path.split("/")
        splited[4] = "meta"

        return "/".join(splited)
    def load_with_athena_query(self, query, format_type="auto", request_limit=10000):
        def get_format_type(query):
            if query.find("SELECT") != -1 or query.find("select") != -1:
                return "select"
            else:
                return "raw"


        def get_var_char_values(d):
            return [obj['VarCharValue'] for obj in d['Data']]

        def parse(header, rows):
            header = get_var_char_values(header)
            return [dict(zip(header, get_var_char_values(row))) for row in rows]

        athena_client = self._get_athena_client()
        if format_type == "auto":
            format_type = get_format_type(query)

        result = []

        print("query start")
        query_id = athena_client.start_query_execution(**{
            "QueryString": query,
            "ResultConfiguration": {
                "OutputLocation": f"s3://{self.bucket_name}/trash/athena_result"
            }
        })["QueryExecutionId"]

        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_id)[
                "QueryExecution"]["Status"]["State"]
            time.sleep(1)
            if status == "CANCELLED":
                raise ValueError(f"[{query}] is cancelled.")

            elif status == "FAILED":
                raise ValueError(f"[{query}] is failed.")

            elif status == "SUCCEEDED":
                break
            else:
                continue
        print("query complete")
        print("get query results ...")
        next_tokens = []
        header = []
        for index in range(request_limit):
            
            request_data = {
                "QueryExecutionId":query_id
            }

            if len(next_tokens):
                request_data["NextToken"] = next_tokens[-1]

            query_raw_result = athena_client.get_query_results(**request_data)

            if format_type == "raw":
                result.append(query_raw_result)
                break
            elif format_type == "select":
                if index == 0:
                    header, *rows = query_raw_result['ResultSet']['Rows']
                else:
                    rows = query_raw_result['ResultSet']['Rows']

                result.extend(parse(header, rows))
                
                if "NextToken" in query_raw_result:
                    new_next_token = query_raw_result["NextToken"]
                    print(f"{index}", end=" ")
                    if new_next_token not in next_tokens:
                        next_tokens.append(new_next_token)
                    else:
                        break
                else:
                    break

        print("\nathena query complete")
        return result


    # TOOLS
    def _parse_full_path(self, full_path):

        is_gz = False
        if full_path[-3:] == ".gz":
            full_path = full_path[:-3]
            is_gz = True

        self._valid_full_path(full_path)

        basename = os.path.basename(full_path)

        _, ext = os.path.splitext(basename)
        splited = full_path.split("/")

        assert splited[0] == "default"
        assert splited[1] == self.warehouse_name

        store_kind = splited[3]
        meta_or_value = splited[4]
        table_name = splited[5]
        ymd = splited[6]
        # splited = uuid.split("_")
        # time_ns = ""
        # if len(splited) == 2:
        #     time_ns, _ = splited

        return types.SimpleNamespace(
            full_path=full_path,
            # time_ns=time_ns,
            # uuid=uuid,
            ext=ext,
            store_kind=store_kind,
            meta_or_value=meta_or_value,
            table_name=table_name,
            gz=is_gz,
            ymd=ymd)

    def _load_worker(self, store_kind, table_name, user_path, table_kind, ymd="", is_meta=False):
        table_name = self._get_table_name(table_name)
        result = None

        warehouse_full_path = self._get_warehouse_full_path(
            store_kind, table_name, user_path, table_kind, ymd, is_meta)
        result = self._es_handler.load(warehouse_full_path)

        return result

    def _list_worker(self, store_kind, table_kind, table_name="", user_dir="", ymd="", load=False, is_meta=False, includes=[], is_direoctry=False):
        result = None
        table_name = self._get_table_name(table_name)
        ymd_with_slash = ""
        user_dir_with_slash = ""
        if user_dir != "":
            user_dir_with_slash = f"/{user_dir}"

        if ymd != "":
            ymd_with_slash = f"/{ymd}"

        full_path = f"{store_kind}/{table_kind}/value/{table_name}{user_dir_with_slash}{ymd_with_slash}"
        objects = self._es_handler.list_objects(full_path, self.warehouse_name)
        if len(includes) > 0:
            temp = []
            for obj in objects:
                key = obj["Key"]
                for include_str in includes:
                    if key.find(include_str) != -1:
                        temp.append(obj)

            objects = temp
        if not is_direoctry:
            objects = self._es_handler.list_objects(full_path, self.warehouse_name)        
            if load == True:
                result = []
                for obj in objects:

                    key = obj["Key"]
                    if is_meta:
                        value = self.load_meta_with_full_path(key)
                    else:
                        value = self.load_with_full_path(key)

                    result.append({
                        "key": key,
                        "value": value
                    })
            else:
                result = objects
        else:
            result = self._es_handler.listdir(full_path)

        return result

    def _get_random_string(self, length=10):
        """
        랜덤으로 문자열을 생성해주는 함수입니다. 길이를 지정 할 수 있습니다.
        """        
        random_box = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
        random_box_length = len(random_box)
        result = ""
        for _ in range(length):
            result += random_box[int(random.random()*random_box_length)]

        return result


    def _json_to_csv(self, json_data):
        """
        json 을 csv string 으로 바꿔줍니다.
        """        
        result = ""
        if not isinstance(json_data, list):
            raise ValueError("json data's type must be list.")
        line = StringIO()
        writer = csv.writer(line)

        if len(json_data) > 0:
            csv_rows = []
            csv_rows.append(list(json_data[0].keys()))
            for row in json_data:
                csv_rows.append(row.values())
            writer.writerows(csv_rows)
            result = line.getvalue()

        return result


    def _get_athena_client(self):
        if self._athena_client == None:
            self._athena_client = boto3.client('athena',
                                               aws_access_key_id=self._aws_access_key_id,
                                               aws_secret_access_key=self._aws_secret_access_key,
                                               region_name=self._region_name)

        return self._athena_client

    def _get_table_name(self, table_name):
        return table_name if table_name else self.table_name
        

    def _get_warehouse_full_path(self, store_kind, table_name, user_path, table_kind="general", ymd="", is_meta=False):
        table_name = self._get_table_name(table_name)

        ymd_with_slash = ""
        if ymd != "":
            ymd_with_slash = "/" + ymd

        if is_meta:
            meta_or_value = "meta"
        else:
            meta_or_value = "value"
        _, ext = os.path.splitext(user_path)
        add_ext = ""
        if ext != ".parquet":
            add_ext = ".gz"

        remaining_path = f"{table_kind}/{meta_or_value}/{table_name}{ymd_with_slash}/{user_path}{add_ext}"
        if store_kind == "raw":
            return f"raw/{remaining_path}"

        if store_kind == "discovery":
            return f"discovery/{remaining_path}"

    def _get_valid_user_path(self, user_path):

        result = ""

        if len(user_path) == 0:
            raise ValueError(f"invalid user_path {user_path}")

        # _, ext = os.path.splitext(user_path)

        if user_path[0] == ".":
            if len(user_path) == 1:
                raise ValueError(f"invalid user_path {user_path}")

            uuid = str(time.time_ns()) + "_" + self._get_random_string(5)
            result = uuid + user_path
        else:
            result = user_path
        return result

    def _upgrade_meta(self, store_kind, meta, warehouse_full_path, table_name):
        table_name = self._get_table_name(table_name)

        temp_meta = meta.copy()
        new_meta = {}
        new_meta.update({
            "default_table_name": table_name
        })
        full_path = f"default/{self.warehouse_name}/{warehouse_full_path}"
        # parsed = self._parse_full_path(full_path)

        temp_meta["full_path"] = full_path
        # temp_meta["uuid"] = parsed.uuid
        temp_meta["stored_time"] = str(datetime.datetime.now())
        # temp_meta["time_ns"] = parsed.time_ns
        temp_meta["ymd"] = datetime.datetime.now().strftime("%Y-%m-%d")

        if store_kind == "raw":
            pass
        elif store_kind == "discovery":
            pass
        else:
            raise ValueError(f"invalid store_kind {store_kind}")

        for key in temp_meta:
            value = temp_meta[key]
            new_meta[f"{store_kind}_{key}"] = value

        return new_meta

    def _valid_full_path(self, full_path):
        if not full_path.startswith(f"default/{self.warehouse_name}/"):
            raise ValueError(f"invalid full_path {full_path}")

    # SAVE
    def _save_worker(self, store_kind, table_kind, user_path, value, meta, use_ymd, table_name, user_ymd):
        result = ""

        ymd = ""
        table_name = self._get_table_name(table_name)
        if user_ymd != "":
            ymd = user_ymd
        elif use_ymd:
            ymd = datetime.datetime.now().strftime("%Y-%m-%d")

        user_path = self._get_valid_user_path(user_path)
        _, ext = os.path.splitext(user_path)

            
        if ext == ".csv" and isinstance(value, list):
            value = self._json_to_csv(value)

        warehouse_full_path = self._get_warehouse_full_path(store_kind, table_name, user_path,
                                                  table_kind=table_kind, ymd=ymd, is_meta=False)

        meta_warehouse_full_path = self._get_warehouse_full_path(store_kind, table_name, user_path,
                                                       table_kind=table_kind, ymd=ymd, is_meta=True)
        
        meta = self._upgrade_meta(store_kind, meta, warehouse_full_path, table_name)

        option = {}
        if ext != ".parquet":
            option = {
                "compress_type": "gz"
            }


        if store_kind == "raw":
            self._es_handler.save(meta_warehouse_full_path, meta, option)

            result = self._es_handler.save(warehouse_full_path, value, option)

        elif store_kind == "discovery":

            result = self._es_handler.save(warehouse_full_path, value, option)

            self._es_handler.save(meta_warehouse_full_path, meta, option)                
        else:
            raise ValueError(f"invalid store kind {store_kind}")

        return result
