// Animation for the ASCII logo
function initializeLogoAnimations() {
    const horizontalSymbols = [" ", "*", "+", "-", "~"];
    const verticalSymbols = [" ", "*", "+", "|"];
    const edgeSymbols = [" ", "*", "+"];

    function getRandomItem(arr) {
        return arr[Math.floor(Math.random() * arr.length)];
    }

    function getRandomDuration(min) {
        return Math.random() * (10000 - min) + min;
    }

    function updateSymbol(symbol, choices) {
        symbol.innerText = getRandomItem(choices);
        setTimeout(() => updateSymbol(symbol, choices), getRandomDuration(500));
    }

    document.querySelectorAll('.horizontal-logo-symbol').forEach(symbol => {
        setTimeout(() => updateSymbol(symbol, horizontalSymbols), getRandomDuration(1500));
    });

    document.querySelectorAll('.vertical-logo-symbol').forEach(symbol => {
        setTimeout(() => updateSymbol(symbol, verticalSymbols), getRandomDuration(1500));
    });

    document.querySelectorAll('.edge-logo-symbol').forEach(symbol => {
        setTimeout(() => updateSymbol(symbol, edgeSymbols), getRandomDuration(1500));
    });
}

// Initialize animations when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    initializeLogoAnimations();

    // Add copyright year to footer if exists
    const footerYear = document.getElementById('footer-year');
    if (footerYear) {
        footerYear.textContent = new Date().getFullYear();
    }
});