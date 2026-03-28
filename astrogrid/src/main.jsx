import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';

const container = document.getElementById('root');

if (!container) {
    throw new Error('AstroGrid root container was not found.');
}

createRoot(container).render(<App />);
