
curl 'http://127.0.0.1:8080/delete?key=1'
curl 'http://127.0.0.1:8080/delete?key=2'
curl 'http://127.0.0.1:8080/set?key=2&value=not2'
curl 'http://127.0.0.1:8080/set?key=1&value=not1'
curl 'http://127.0.0.1:8080/set?key=2&value=incorrect'
curl 'http://127.0.0.1:8080/set?key=1&value=incorrect'
curl 'http://127.0.0.1:8080/delete?key=1'
curl 'http://127.0.0.1:8080/delete?key=2'
curl 'http://127.0.0.1:8080/set?key=1&value=1'
curl 'http://127.0.0.1:8080/set?key=2&value=2'
res=$(curl -s 'http://127.0.0.2:8080/get?key=2')
echo "$res"
res=$(curl -s 'http://127.0.0.3:8080/get?key=2')
echo "$res"
res=$(curl -s 'http://127.0.0.2:8080/get?key=1')
echo "$res"
res=$(curl -s 'http://127.0.0.3:8080/get?key=1')
echo "$res"
