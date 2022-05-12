import os
import sys
import time
from pathlib import Path
import datetime

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from sentinelhub import SHConfig
from sentinelhub import MimeType, CRS, BBox, filter_times, bbox_to_dimensions
from sentinelhub import SentinelHubCatalog, SentinelHubRequest, SentinelHubDownloadClient, DataCollection
# from sentinelhub import DownloadRequest

sentinel_dc_info = {
    "SENTINEL2_L1C": DataCollection.SENTINEL2_L1C,
    "SENTINEL2_L2A": DataCollection.SENTINEL2_L2A,
    "SENTINEL1": DataCollection.SENTINEL1,
    "SENTINEL1_IW": DataCollection.SENTINEL1_IW,
    "SENTINEL1_IW_ASC": DataCollection.SENTINEL1_IW_ASC,
    "SENTINEL1_IW_DES": DataCollection.SENTINEL1_IW_DES,
    "SENTINEL1_EW": DataCollection.SENTINEL1_EW,
    "SENTINEL1_EW_ASC": DataCollection.SENTINEL1_EW_ASC,
    "SENTINEL1_EW_DES": DataCollection.SENTINEL1_EW_DES,
    "SENTINEL1_EW_SH": DataCollection.SENTINEL1_EW_SH,
    "SENTINEL1_EW_SH_ASC": DataCollection.SENTINEL1_EW_SH_ASC,
    "SENTINEL1_EW_SH_DES": DataCollection.SENTINEL1_EW_SH_DES,
    "DEM": DataCollection.DEM,
    "MODIS": DataCollection.MODIS,
    "LANDSAT8": DataCollection.LANDSAT8,
    "SENTINEL5P": DataCollection.SENTINEL5P,
    "SENTINEL3_OLCI": DataCollection.SENTINEL3_OLCI,
    "SENTINEL5P": DataCollection.SENTINEL5P,
}

data_types = {
    "TIFF": MimeType.TIFF,
    "PNG": MimeType.PNG,
    "JPG": MimeType.JPG,
    "JP2": MimeType.JP2,
    "JSON": MimeType.JSON,
    "CSV": MimeType.CSV,
    "ZIP": MimeType.ZIP,
    "HDF": MimeType.HDF,
    "XML": MimeType.XML,
    "GML": MimeType.GML,
    "TXT": MimeType.TXT,
    "TAR": MimeType.TAR,
    "RAW": MimeType.RAW,
    "SAFE": MimeType.SAFE
}


def read_paras(tgt_file):
    try:
        tree = ET.parse(tgt_file)  # 'sentinel_download.xml'
        root = tree.getroot()
        # for child in root:
        #     print(child.tag, child.attrib, child.text)
    except Exception as e:
        print(e)
        sys.exit(0)

    instance_id_code = root.find('instance_id')
    client_id_code = root.find('sh_client_id')
    client_secret_code = root.find('sh_client_secret')

    save_dir = root.find("save_dir").text
    data_resolution = float(root.find("resolution").text)
    tmp_coords = [
        float(item) for item in root.find("area_coords").text.split(',')
    ]

    lon_tmp = tmp_coords[0::2]
    lat_tmp = tmp_coords[1::2]
    data_coords_wgs84 = [
        min(lon_tmp), min(lat_tmp),
        max(lon_tmp), max(lon_tmp)
    ]
    print("data area coord : ", data_coords_wgs84)

    begin_time = root.find("begin_time").text
    end_time = root.find("end_time").text

    datacol = root.find("DataCollection").text
    if datacol not in sentinel_dc_info.keys():
        print(f"No such a data collection : {datacol}")
        sys.exit(0)
    dc = sentinel_dc_info[datacol]

    dtype = root.find("DataType").text.upper()
    if dtype in ['TIF', 'TIFF']:
        dtype = 'TIFF'
    if dtype not in data_types.keys():
        print(f"No such a data collection : {dtype}")
        sys.exit(0)
    mtype = data_types[dtype]

    tmp_set = [
        instance_id_code, client_id_code, client_secret_code, save_dir,
        data_resolution, data_coords_wgs84, begin_time, end_time, dc, mtype
    ]

    return tmp_set


def config_init(instance_id, sh_client_id, sh_client_secret):
    """
    instance_id = 'ba5dc32a-5629-40ee-a6e2-ae26d3d13d37'
    sh_client_id = '6cc8c4ed-fd7f-458b-8d21-b5bf6d101079'
    sh_client_secret = 'CJpf@}8UAj@|:m&(3#LboT1HUjf}A+5%35s_Fn.s'
    """
    config = SHConfig()
    config.instance_id = instance_id
    config.sh_client_id = sh_client_id
    config.sh_client_secret = sh_client_secret
    if not config.sh_client_id or not config.sh_client_secret:
        print(
            "Warning! To use Process API, please provide the credentials (OAuth client ID and client secret)."
        )
    else:
        print(f"current user : {sh_client_id}")
    return config


def creare_areainfo(area_coords_wgs84, resolution=10, crs=CRS.WGS84):
    area_bbox = BBox(bbox=area_coords_wgs84, crs=crs)
    area_size = bbox_to_dimensions(area_bbox, resolution=resolution)
    print(f'Image shape at {resolution} m resolution: {area_size} pixels')
    return area_bbox, area_size


def catalog_search(current_config,
                   data_bbox,
                   time_interval,
                   datacollection,
                   time_delta=datetime.timedelta(days=1)):
    catalog = SentinelHubCatalog(config=current_config)
    search_iterator = catalog.search(
        datacollection,  # DataCollection.SENTINEL2_L1C,
        bbox=data_bbox,
        time=time_interval,
        query={"eo:cloud_cover": {
            "lt": 5
        }},
        fields={
            "include":
            ["id", "properties.datetime", "properties.eo:cloud_cover"],
            "exclude": []
        })
    all_timestamps = search_iterator.get_timestamps()
    unique_acquisitions = filter_times(all_timestamps, time_delta)
    # for idx, item in enumerate(unique_acquisitions):
    #     print(f"{idx} data time {item}")
    print('Total number of results:', len(list(search_iterator)))
    data_fn = [item['id'] for item in list(search_iterator)]
    return unique_acquisitions, data_fn[::-1]


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print("No useful parameter for input!")
        sys.exit(0)
    else:
        xml_file = sys.argv[1]

    print(Path.cwd())

    xml_file = xml_file.strip()
    xml_file = xml_file.strip(os.sep)
    xml_file = Path(xml_file)

    if not xml_file.exists():
        print(f"No such a file {xml_file}!")
        sys.exit(0)

    paras = read_paras(xml_file)
    instance_id_code, client_id_code, client_secret_code, save_dir, data_resolution, data_coords_wgs84, begin_time, end_time, dc, mtype = paras

    print("begin to downlaod : ",
          time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    time_start = time.time()

    # 初始换账户信息
    current_config = config_init(instance_id_code.text, client_id_code.text,
                                 client_secret_code.text)

    # 初始换下载端口
    client = SentinelHubDownloadClient(config=current_config)

    # 计算数据区域与大小
    data_bbox, data_size = creare_areainfo(data_coords_wgs84,
                                           resolution=data_resolution)

    # 设置搜索时间区间和时间间隔，默认为1天
    time_interval = (begin_time, end_time)
    time_difference = datetime.timedelta(days=1)  # hours=8

    # 搜索数据
    unique_acquisitions, data_fn = catalog_search(current_config,
                                                  data_bbox,
                                                  time_interval,
                                                  dc,
                                                  time_delta=time_difference)

    evalscript_all_bands = """
    //VERSION=3
    function setup() {
        return {
            input: [{
                bands: ["B01","B02","B03","B04","B05","B06","B07","B08","B8A","B09","B10","B11","B12"],
                units: "DN"
            }],
            output: {
                bands: 13,
                sampleType: "INT16"
            }
        };
    }

    function evaluatePixel(sample) {
        return [sample.B01,
                sample.B02,
                sample.B03,
                sample.B04,
                sample.B05,
                sample.B06,
                sample.B07,
                sample.B08,
                sample.B8A,
                sample.B09,
                sample.B10,
                sample.B11,
                sample.B12];
    }
    """

    process_requests = []
    for idx, timestamp in enumerate(unique_acquisitions):
        print("{} {} {}".format(idx, timestamp, data_fn[idx]))
        request = SentinelHubRequest(
            data_folder=save_dir,
            evalscript=evalscript_all_bands,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=dc,  # DataCollection.SENTINEL2_L1C,
                    time_interval=(timestamp - time_difference,
                                   timestamp + time_difference))
            ],
            responses=[
                SentinelHubRequest.output_response('default',
                                                   mtype),  # MimeType.TIFF
                #SentinelHubRequest.output_response('userdata', MimeType.JSON)
            ],
            bbox=data_bbox,
            size=data_size,
            config=current_config)
        process_requests.append(request)

    download_requests = [
        request.download_list[0] for request in process_requests
    ]
    data = client.download(download_requests, max_threads=5)

    download_name = [
        request.get_filename_list()[0] for request in process_requests
    ]

    print("data save path:")
    for idx, (tmp_name, true_name) in enumerate(zip(download_name, data_fn)):
        new_path = Path(save_dir) / true_name
        old_path = (Path(save_dir) / tmp_name).parent
        os.rename(str(old_path), str(new_path))
        Path.rename(new_path / "response.tiff",
                    new_path / (true_name + ".tiff"))
        Path.rename(new_path / "request.json",
                    new_path / (true_name + ".json"))
        print("{} {}".format(idx, new_path / (true_name + ".tiff")))

    print("All files have been downlaod ",
          time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    time_end = time.time()
    print('totally cost : {:.4f}mins'.format((time_end - time_start) / 60))

# # request_all_bands = SentinelHubRequest(
# #     data_folder='test_dir',
# #     evalscript=evalscript_all_bands,
# #     input_data=[
# #         SentinelHubRequest.input_data(
# #             data_collection=DataCollection.SENTINEL2_L1C,
# #             time_interval=('2019-01-01', '2019-12-30'),
# #             mosaicking_order='leastCC')
# #     ],
# #     responses=[SentinelHubRequest.output_response('default', MimeType.TIFF)],
# #     bbox=betsiboka_bbox,
# #     size=betsiboka_size,
# #     config=current_config)

# # all_bands_img = request_all_bands.save_data()
# # print(type(request_all_bands.data_folder))
