let scanInterval = null;

async function startScan() {
    const fromDate = document.getElementById('fromDate').value;
    const toDate = document.getElementById('toDate').value;
    const expiryDate = document.getElementById('expiryDate').value;

    if (!fromDate || !toDate || !expiryDate) {
        alert('Please fill in all date fields');
        return;
    }

    // Disable button and show progress
    document.getElementById('startScanBtn').disabled = true;
    document.getElementById('progressSection').classList.remove('hidden');
    document.getElementById('progressText').textContent = 'Starting scan...';

    try {
        const response = await fetch('/api/start_scan', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                from_date: fromDate,
                to_date: toDate,
                expiry_date: expiryDate
            })
        });

        const result = await response.json();

        if (response.ok) {
            // Start polling for status
            pollScanStatus();
        } else {
            throw new Error(result.message || 'Failed to start scan');
        }
    } catch (error) {
        console.error('Error starting scan:', error);
        alert(`Error: ${error.message}`);
        document.getElementById('startScanBtn').disabled = false;
        document.getElementById('progressSection').classList.add('hidden');
    }
}

function pollScanStatus() {
    scanInterval = setInterval(async () => {
        try {
            const response = await fetch('/api/scan_status');
            const status = await response.json();

            document.getElementById('progressText').textContent =
                `Status: ${status.status} (${status.progress}%)`;
            document.getElementById('progressFill').style.width = `${status.progress}%`;

            if (status.status === 'completed') {
                clearInterval(scanInterval);
                await loadResults();
                document.getElementById('startScanBtn').disabled = false;
            } else if (status.status === 'error') {
                clearInterval(scanInterval);
                document.getElementById('progressText').textContent = 'Error occurred during scan';
                document.getElementById('startScanBtn').disabled = false;
            }
        } catch (error) {
            console.error('Error polling status:', error);
            clearInterval(scanInterval);
        }
    }, 2000); // Poll every 2 seconds
}

async function loadResults() {
    try {
        const response = await fetch('/api/results');
        const results = await response.json();

        if (response.ok && Array.isArray(results)) {
            displayResults(results);
        } else {
            throw new Error('Invalid results format');
        }
    } catch (error) {
        console.error('Error loading results:', error);
        document.getElementById('resultsBody').innerHTML =
            `<tr><td colspan="8">Error loading results: ${error.message}</td></tr>`;
    }
}

function displayResults(results) {
    const tbody = document.getElementById('resultsBody');
    tbody.innerHTML = '';

    if (results.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8">No OTM options with significant OI growth found</td></tr>';
        document.getElementById('resultsSection').classList.remove('hidden');
        return;
    }

    results.forEach(result => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${result.symbol}</td>
            <td>${result.strike_price}</td>
            <td>${result.option_type}</td>
            <td>${Math.round(result.initial_oi)}</td>
            <td>${Math.round(result.final_oi)}</td>
            <td>${Math.round(result.total_growth)}</td>
            <td>${(result.total_pct_growth * 100).toFixed(2)}%</td>
            <td>${result.consistency_score ? result.consistency_score.toFixed(4) : '0.0000'}</td>
        `;
        tbody.appendChild(row);
    });

    document.getElementById('resultsSection').classList.remove('hidden');
}