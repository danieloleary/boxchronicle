class BoxChronicleMonitor {
    constructor() {
        this.statusIndicator = document.getElementById('statusIndicator');
        this.statusText = document.getElementById('statusText');
        this.lastRunTime = document.getElementById('lastRunTime');
        this.nextRunTime = document.getElementById('nextRunTime');
        this.runNowBtn = document.getElementById('runNowBtn');
        this.totalEvents = document.getElementById('totalEvents');
        this.eventsToday = document.getElementById('eventsToday');
        this.lastBatchSize = document.getElementById('lastBatchSize');
        this.activityLog = document.getElementById('activityLog');
        this.lastUpdated = document.getElementById('lastUpdated');

        this.initializeEventListeners();
        this.startPolling();
    }

    initializeEventListeners() {
        this.runNowBtn.addEventListener('click', () => this.triggerManualRun());
    }

    async startPolling() {
        await this.updateStatus();
        setInterval(() => this.updateStatus(), 30000); // Update every 30 seconds
    }

    async updateStatus() {
        try {
            const response = await fetch('/api/status');
            const data = await response.json();
            this.updateUI(data);
        } catch (error) {
            console.error('Error fetching status:', error);
            this.setErrorState();
        }
    }

    updateUI(data) {
        // Update status indicator
        this.statusIndicator.className = 'status-indicator';
        this.statusIndicator.classList.add(data.status.toLowerCase());
        this.statusText.textContent = this.formatStatus(data.status);

        // Update times
        this.lastRunTime.textContent = this.formatTime(data.lastRun);
        this.nextRunTime.textContent = this.formatTime(data.nextRun);

        // Update statistics
        this.totalEvents.textContent = data.stats.totalEvents.toLocaleString();
        this.eventsToday.textContent = data.stats.eventsToday.toLocaleString();
        this.lastBatchSize.textContent = data.stats.lastBatchSize.toLocaleString();

        // Update activity log
        this.updateActivityLog(data.recentActivity);

        // Update last updated time
        this.lastUpdated.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;

        // Update run button state
        this.runNowBtn.disabled = data.isRunning;
    }

    updateActivityLog(activities) {
        this.activityLog.innerHTML = activities.map(activity => `
            <tr>
                <td>${this.formatTime(activity.timestamp)}</td>
                <td>${activity.type}</td>
                <td><span class="status-badge ${activity.status.toLowerCase()}">${activity.status}</span></td>
                <td>${activity.details}</td>
            </tr>
        `).join('');
    }

    async triggerManualRun() {
        try {
            this.runNowBtn.disabled = true;
            this.runNowBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Running...';
            
            const response = await fetch('/api/run', { method: 'POST' });
            const data = await response.json();
            
            if (data.success) {
                this.showNotification('Integration started successfully', 'success');
            } else {
                this.showNotification('Failed to start integration', 'error');
            }
        } catch (error) {
            console.error('Error triggering manual run:', error);
            this.showNotification('Error starting integration', 'error');
        } finally {
            this.runNowBtn.disabled = false;
            this.runNowBtn.innerHTML = '<i class="bi bi-play-fill"></i> Run Now';
        }
    }

    setErrorState() {
        this.statusIndicator.className = 'status-indicator error';
        this.statusText.textContent = 'Error fetching status';
        this.lastUpdated.textContent = 'Last updated: Error';
    }

    formatStatus(status) {
        const statusMap = {
            'ACTIVE': 'Running',
            'ERROR': 'Error',
            'WARNING': 'Warning',
            'IDLE': 'Idle'
        };
        return statusMap[status] || status;
    }

    formatTime(timestamp) {
        if (!timestamp) return '-';
        const date = new Date(timestamp);
        return date.toLocaleString();
    }

    showNotification(message, type = 'info') {
        // You can implement a notification system here
        console.log(`${type.toUpperCase()}: ${message}`);
    }
}

// Initialize the monitor when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new BoxChronicleMonitor();
}); 