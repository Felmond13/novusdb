// Tab switching for code examples
function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.code-panel').forEach(p => p.classList.remove('active'));
  
  document.querySelector(`#tab-${name}`).classList.add('active');
  
  // Find the clicked tab button
  document.querySelectorAll('.tab').forEach(t => {
    if (t.textContent.toLowerCase().replace('.', '') === name || 
        (name === 'node' && t.textContent === 'Node.js') ||
        (name === 'cli' && t.textContent === 'CLI')) {
      t.classList.add('active');
    }
  });
}

// Smooth reveal on scroll
const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
    }
  });
}, { threshold: 0.1 });

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.feature-card, .driver-card, .price-card, .stat').forEach(el => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(20px)';
    el.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
    observer.observe(el);
  });
});

// Add visible class styles
const style = document.createElement('style');
style.textContent = '.visible { opacity: 1 !important; transform: translateY(0) !important; }';
document.head.appendChild(style);

// Staggered animation delay
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.features-grid, .drivers-grid, .stats-grid, .pricing-grid').forEach(grid => {
    grid.querySelectorAll(':scope > *').forEach((child, i) => {
      child.style.transitionDelay = `${i * 0.08}s`;
    });
  });
});

// Nav background on scroll
window.addEventListener('scroll', () => {
  const nav = document.querySelector('.nav');
  if (window.scrollY > 50) {
    nav.style.background = 'rgba(10, 10, 15, 0.95)';
  } else {
    nav.style.background = 'rgba(10, 10, 15, 0.8)';
  }
});
