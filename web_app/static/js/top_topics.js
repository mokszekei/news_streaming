var ctx = document.getElementById('top_topics').getContext('2d');

var top_topics_chart = new Chart(ctx, {
    type: 'horizontalBar',
    data: {
        labels: [],

        datasets: [{
            label: "count",
            data: [],
            backgroundColor: "rgba(194, 67, 67, 0.6)"
        }]
    },
    options: {
        scales: {
            xAxes: [{
                stacked: true,
                ticks: {
                    beginAtZero: true,
                    callback: function(value) {if (value % 1 === 0) {return value;}}
                }
            }],
            yAxes: [{
                stacked: true
            }]
        }
    }
});

var src_top_topics= {
    labels: [],
    count: []
}

setInterval(function(){
    $.getJSON('/refresh_top_topics', {
    }, function(data) {
        src_top_topics.labels = data.sLabel;
        src_top_topics.count = data.sCount;
    });
    top_topics_chart.data.labels = src_top_topics.labels;
    top_topics_chart.data.datasets[0].data = src_top_topics.count;
    top_topics_chart.update();
},2000);