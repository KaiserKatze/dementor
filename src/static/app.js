
let StartSpider = () => {
    // Send a RPC to server
    console.log('StartSpider();');

    let xhr;

    if (window.XMLHttpRequest) {
        xhr = new XMLHttpRequest();
    } else {
        xhr = new ActiveXObject('Microsoft.XMLHTTP');
    }

    xhr.open('POST', '/ajax/spider/start/', true);
    xhr.send();
};