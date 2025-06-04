const express = require('express');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// State management
let state = {
    lastRun: null,
    nextRun: null,
    isRunning: false,
    stats: {
        totalEvents: 0,
        eventsToday: 0,
        lastBatchSize: 0
    },
    recentActivity: []
};

// Helper function to update stats from log file
function updateStatsFromLogs() {
    try {
        const logDir = path.join(__dirname, 'logs');
        if (!fs.existsSync(logDir)) return;

        const today = new Date().toISOString().split('T')[0];
        const logFile = path.join(logDir, `boxchronicle_${today}.log`);
        
        if (!fs.existsSync(logFile)) return;

        const logContent = fs.readFileSync(logFile, 'utf8');
        const lines = logContent.split('\n');
        
        // Count total events
        const eventLines = lines.filter(line => line.includes('Successfully processed'));
        state.stats.totalEvents = eventLines.reduce((sum, line) => {
            const match = line.match(/processed (\d+) events/);
            return sum + (match ? parseInt(match[1]) : 0);
        }, 0);

        // Count today's events
        const todayLines = eventLines.filter(line => line.includes(today));
        state.stats.eventsToday = todayLines.reduce((sum, line) => {
            const match = line.match(/processed (\d+) events/);
            return sum + (match ? parseInt(match[1]) : 0);
        }, 0);

        // Get last batch size
        const lastEventLine = eventLines[eventLines.length - 1];
        if (lastEventLine) {
            const match = lastEventLine.match(/processed (\d+) events/);
            state.stats.lastBatchSize = match ? parseInt(match[1]) : 0;
        }

        // Update recent activity
        state.recentActivity = eventLines.slice(-10).map(line => {
            const timestamp = line.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/)?.[1];
            const eventCount = line.match(/processed (\d+) events/)?.[1];
            return {
                timestamp: timestamp ? new Date(timestamp).toISOString() : null,
                type: 'Event Processing',
                status: 'SUCCESS',
                details: `Processed ${eventCount} events`
            };
        }).filter(activity => activity.timestamp);
    } catch (error) {
        console.error('Error updating stats:', error);
    }
}

// API Endpoints
app.get('/api/status', (req, res) => {
    updateStatsFromLogs();
    res.json({
        status: state.isRunning ? 'ACTIVE' : 'IDLE',
        lastRun: state.lastRun,
        nextRun: state.nextRun,
        isRunning: state.isRunning,
        stats: state.stats,
        recentActivity: state.recentActivity
    });
});

app.post('/api/run', async (req, res) => {
    if (state.isRunning) {
        return res.status(400).json({ success: false, message: 'Integration is already running' });
    }

    try {
        state.isRunning = true;
        state.lastRun = new Date().toISOString();
        
        // Spawn Python process
        const pythonProcess = spawn('python', ['main.py']);
        
        pythonProcess.stdout.on('data', (data) => {
            console.log(`Python stdout: ${data}`);
        });

        pythonProcess.stderr.on('data', (data) => {
            console.error(`Python stderr: ${data}`);
        });

        pythonProcess.on('close', (code) => {
            state.isRunning = false;
            state.nextRun = new Date(Date.now() + 5 * 60 * 1000).toISOString(); // Next run in 5 minutes
            console.log(`Python process exited with code ${code}`);
        });

        res.json({ success: true, message: 'Integration started' });
    } catch (error) {
        state.isRunning = false;
        console.error('Error starting integration:', error);
        res.status(500).json({ success: false, message: 'Failed to start integration' });
    }
});

// Start server
app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});
