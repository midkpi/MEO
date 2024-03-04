import datetime
import pytz
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

async def timeSity(city_name):
    # Initialize geolocator
    geolocator = Nominatim(user_agent='xxx')

    # Get location information for the city in Russia
    location = geolocator.geocode(city_name + ", Russia")

    if location:
        # Get timezone from coordinates
        tf = TimezoneFinder()
        latitude, longitude = location.latitude, location.longitude

        # Timezone
        timezone_str = tf.timezone_at(lng=longitude, lat=latitude)
        timezone = pytz.timezone(timezone_str)

        # Get current date and time in the city's timezone
        global_date = datetime.datetime.now(timezone)

        return city_name + ": " + global_date.strftime('%H:%M:%S %Z')
    else:
        return "Город не найден."
