var mymap = L.map('mapid').setView([40.712800, -74.005900], 11);
L.tileLayer('https://api.mapbox.com/styles/v1/beizhao215/ciu2cjsga006e2iqd93u9vfe4/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1IjoiYmVpemhhbzIxNSIsImEiOiJjaXExcXhkM3UwMHlhZnNubm5uaG8wenJnIn0.cAFVYL166sWTuqwT_zwSzg', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'insight'
    //accessToken: 'pk.eyJ1IjoiYmVpemhhbzIxNSIsImEiOiJjaXExcXhkM3UwMHlhZnNubm5uaG8wenJnIn0.cAFVYL166sWTuqwT_zwSzg'
}).addTo(mymap);



mymap.on('click', onMapClick);
var lat
var lng
var currentdate = new Date();
var popup = L.popup();

function onMapClick(e) {
    var latlon = e.latlng;
    lat = latlon.lat;
    lng = latlon.lng;
    currenttime = currentdate.getHours() + ":" + currentdate.getMinutes()
    var a = $("#data").text(lat);
    $('p[name=a]').text("Latitude: "+lat);
    $('p[name=b]').text("Longitude: "+lng);
    $('p[name=c]').text("Time: "+currenttime);
    popup
        .setLatLng(e.latlng)
        .setContent("You clicked the map at " + e.latlng.toString())
        .openOn(mymap);
}



