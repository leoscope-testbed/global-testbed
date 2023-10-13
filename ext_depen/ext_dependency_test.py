from common.utils import get_public_ip, get_weather_mon_info, get_satellite_info

def test_server_ip_addr():
    get_public_ip()

def test_weather_info():
    i_api, i_lat, i_lon, i_ele = get_weather_mon_info()
    print("api = %s, lat = %s , lon = %s, ele = %s", i_api, i_lat, i_lon, i_ele)

def test_get_satellite_info():
    sat_info = get_satellite_info()
    print("Satellite info : " , sat_info)

def test_ext_dependency():
    test_server_ip_addr()
    test_weather_info()
    test_get_satellite_info()