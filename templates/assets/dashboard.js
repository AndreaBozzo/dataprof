// Dashboard Interactive Features

function toggleFileDetails(fileId) {
    const detailsRow = document.getElementById('details-' + fileId);
    if (detailsRow) {
        if (detailsRow.style.display === 'none' || detailsRow.style.display === '') {
            detailsRow.style.display = 'table-row';
        } else {
            detailsRow.style.display = 'none';
        }
    }
}

// Initialize interactive features when page loads
document.addEventListener('DOMContentLoaded', function() {
    // Add smooth scrolling to anchor links
    const links = document.querySelectorAll('a[href^="#"]');
    links.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });

    // Add tooltips to score badges
    const scoreBadges = document.querySelectorAll('.score-badge');
    scoreBadges.forEach(badge => {
        const score = parseFloat(badge.textContent);
        let tooltip = '';
        if (score >= 80) {
            tooltip = 'Excellent data quality';
        } else if (score >= 60) {
            tooltip = 'Good data quality with some issues';
        } else {
            tooltip = 'Poor data quality - needs attention';
        }
        badge.setAttribute('title', tooltip);
    });
});
