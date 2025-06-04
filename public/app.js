document.addEventListener('DOMContentLoaded', () => {
    const button = document.getElementById('testButton');
    if (button) {
        button.addEventListener('click', () => {
            alert('Frontend is working!');
        });
    }
});
