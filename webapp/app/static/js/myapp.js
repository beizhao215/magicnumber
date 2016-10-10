var mymap = L.map('mapid').setView([40.712800, -74.005900], 11);
L.tileLayer('https://api.mapbox.com/styles/v1/beizhao215/ciu2cjsga006e2iqd93u9vfe4/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1IjoiYmVpemhhbzIxNSIsImEiOiJjaXExcXhkM3UwMHlhZnNubm5uaG8wenJnIn0.cAFVYL166sWTuqwT_zwSzg', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'insight'
}).addTo(mymap);

{% for each in output %}


var circle = L.circle([each[0], each[1]], 0.5).addTo(mymap);

{% endfor %}