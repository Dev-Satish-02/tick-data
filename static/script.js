let chart;

async function loadData() {
    const symbol = document.getElementById('symbol').value;
    const res = await fetch(`/data?symbol=${symbol}`);
    const data = await res.json();

    const labels = data.map(d => d.minute);
    const closes = data.map(d => d.close);

    const config = {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: `${symbol} Close Price`,
                data: closes,
                borderColor: 'blue',
                fill: false,
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: { title: { display: true, text: 'Minute' }},
                y: { title: { display: true, text: 'Price' }}
            }
        }
    };

    if (chart) chart.destroy();
    chart = new Chart(document.getElementById('ohlcChart').getContext('2d'), config);
}

async function startPublisher() {
    try {
        const res = await fetch('/start_publisher');
        if (!res.ok) {
            const err = await res.json();
            alert(err.status || 'Failed to start publisher');
        } else {
            const data = await res.json();
            alert(data.status);
        }
    } catch (e) {
        alert("Error: Unable to reach server.");
    }
}

async function startAggregator() {
    const symbol = document.getElementById('symbol').value;
    try {
        const res = await fetch(`/start_aggregator?symbol=${symbol}`);
        if (!res.ok) {
            const err = await res.json();
            alert(err.status || 'Failed to start aggregator');
        } else {
            const data = await res.json();
            alert(data.status);
        }
    } catch (e) {
        alert("Error: Unable to reach server.");
    }
}

async function terminateProcesses() {
    const res = await fetch('/terminate');
    const data = await res.json();
    alert(data.status);
}

async function downloadCSV() {
    window.location.href = '/download_csv';
}

async function updateLogs() {
    const [pubRes] = await Promise.all([
        fetch('/log/publisher'),
    ]);
    const pubData = await pubRes.json();

    document.getElementById('publisherLog').innerHTML = pubData.map(d =>
        `[${d.timestamp}] ${d.symbol} → Price: ${d.price}, Volume: ${d.volume}`
    ).join('<br>');
}

async function loadTable() {
    const res = await fetch('/aggregator_data');
    const data = await res.json();
    const tbody = document.getElementById('dataTable').querySelector('tbody');
    tbody.innerHTML = data.map(row => `
        <tr>
            <td>${row.symbol}</td>
            <td>${row.minute}</td>
            <td>${parseFloat(row.open).toFixed(2)}</td>
            <td>${parseFloat(row.high).toFixed(2)}</td>
            <td>${parseFloat(row.low).toFixed(2)}</td>
            <td>${parseFloat(row.close).toFixed(2)}</td>
            <td>${parseFloat(row.volume).toFixed(2)}</td>
        </tr>
    `).join('');
}

loadData();
loadTable();
setInterval(loadData, 15000);
setInterval(updateLogs, 1000);
setInterval(loadTable, 20000);
